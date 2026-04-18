/* metrics_server.c — Minimal HTTP/1.1 server for GET /metrics.
 *
 * Threading model: one accept thread + per-connection threads (detached).
 * Each connection:
 *   1. Reads until "\r\n\r\n" (end of HTTP headers).
 *   2. If "GET /metrics", render and send 200 OK.
 *   3. Otherwise send 404.
 *   4. Close connection (Connection: close).
 *
 * No keep-alive, no chunked encoding — simple enough for Prometheus scrape.
 */

#define _POSIX_C_SOURCE 200809L

#include "metrics_server.h"
#include "metrics.h"
#include "../cluster/disk_weight.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <poll.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

/* Process start time — set on first tsdb_metrics_server_start call.
 * Used by /health to report uptime in seconds. */
static time_t g_start_epoch = 0;
static pthread_once_t g_once = PTHREAD_ONCE_INIT;
static void record_start(void) { g_start_epoch = time(NULL); }

/* /cluster provider registry (see header).  Atomic-store via pointer
 * writes, which are word-sized on every platform we target. */
static tsdb_cluster_json_fn g_cluster_fn  = NULL;
static void                *g_cluster_ud  = NULL;
static char                 g_data_dir[4096] = {0};

void tsdb_metrics_server_set_cluster_provider(tsdb_cluster_json_fn fn,
                                               void *userdata) {
    g_cluster_ud = userdata;
    g_cluster_fn = fn;
}

void tsdb_metrics_server_set_data_dir(const char *path) {
    if (!path) { g_data_dir[0] = '\0'; return; }
    snprintf(g_data_dir, sizeof(g_data_dir), "%s", path);
}

/* Build the default synthetic /cluster JSON used when no provider is
 * registered.  Reports the single local node with disk capacity. */
static int build_standalone_cluster_json(char *buf, size_t cap) {
    long uptime = (long)(time(NULL) - g_start_epoch);
    if (uptime < 0) uptime = 0;
    uint64_t total = 0, free_b = 0;
    int vn = 0;
    if (g_data_dir[0]) {
        vn = tsdb_disk_weight_detail(g_data_dir,
                                      TSDB_DISK_WEIGHT_DEFAULT_PER_TB,
                                      &total, &free_b);
    }
    char host[128] = "self";
    gethostname(host, sizeof(host) - 1);
    host[sizeof(host) - 1] = '\0';
    return snprintf(buf, cap,
        "{\"mode\":\"standalone\","
        "\"local_id\":0,"
        "\"nodes\":[{"
            "\"id\":0,"
            "\"addr\":\"%s\","
            "\"state\":\"ALIVE\","
            "\"ver\":0,"
            "\"uptime_s\":%ld,"
            "\"pid\":%d,"
            "\"disk\":{\"total_bytes\":%llu,\"free_bytes\":%llu,"
                      "\"data_dir\":\"%s\",\"vn_weight\":%d}"
        "}]}\n",
        host, uptime, (int)getpid(),
        (unsigned long long)total, (unsigned long long)free_b,
        g_data_dir, vn);
}

/* ---- Server struct -------------------------------------------------------- */

struct tsdb_metrics_server {
    int          listen_fd;
    int          port;
    volatile int running;
    pthread_t    accept_thread;
};

/* ---- Connection handler -------------------------------------------------- */

static void write_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, buf + sent, len - sent);
        if (n <= 0) break;
        sent += (size_t)n;
    }
}

static void *handle_connection(void *arg) {
    int fd = (int)(intptr_t)arg;

    /* 5-second read + write deadline — stops a slow-loris-style client
     * from pinning a thread forever.  Pre-dashboard era there was no
     * deadline, which is fine for Prometheus scraping but leaves the
     * server-side thread count unbounded under dashboard reload spam. */
    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    /* Read request headers (stop at \r\n\r\n or buffer full). */
    char req[2048];
    size_t pos = 0;
    int got_end = 0;

    while (pos < sizeof(req) - 1) {
        ssize_t n = read(fd, req + pos, sizeof(req) - 1 - pos);
        if (n <= 0) break;
        pos += (size_t)n;
        req[pos] = '\0';
        if (strstr(req, "\r\n\r\n") || strstr(req, "\n\n")) {
            got_end = 1;
            break;
        }
    }

    if (!got_end) {
        close(fd);
        return NULL;
    }

    /* Parse first line: "GET <path> HTTP/1.x". */
    int route_metrics = 0;
    int route_health  = 0;
    int route_dash    = 0;
    int route_cluster = 0;
    if (strncmp(req, "GET /metrics", 12) == 0)           route_metrics = 1;
    else if (strncmp(req, "GET /health", 11) == 0)       route_health  = 1;
    else if (strncmp(req, "GET /cluster", 12) == 0)      route_cluster = 1;
    else if (strncmp(req, "GET / ", 6) == 0 ||
             strncmp(req, "GET /index", 10) == 0)        route_dash    = 1;

    if (route_metrics) {
        size_t body_len = 0;
        char *body = tsdb_metrics_render(&body_len);
        if (!body) {
            const char *err =
                "HTTP/1.1 500 Internal Server Error\r\n"
                "Content-Length: 0\r\n"
                "Connection: close\r\n\r\n";
            write_all(fd, err, strlen(err));
        } else {
            char hdr[256];
            int hlen = snprintf(hdr, sizeof(hdr),
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                "Content-Length: %zu\r\n"
                "Connection: close\r\n\r\n",
                body_len);
            write_all(fd, hdr, (size_t)hlen);
            write_all(fd, body, body_len);
            free(body);
        }
    } else if (route_cluster) {
        /* Cluster topology.  If the cluster module has registered a
         * provider we defer to it (full member list + autobalance);
         * otherwise we synthesise a single-node JSON so the dashboard
         * has something consistent to render. */
        char body[8192];
        int blen = 0;
        if (g_cluster_fn) blen = g_cluster_fn(g_cluster_ud, body, sizeof(body));
        if (blen <= 0)    blen = build_standalone_cluster_json(body, sizeof(body));
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n\r\n",
            blen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body, (size_t)blen);
    } else if (route_health) {
        /* Minimal liveness payload — kept JSON-only so k8s / curl
         * integrations parse it trivially.  Uptime is seconds since the
         * first metrics_server_start call this process. */
        time_t now = time(NULL);
        long uptime = (long)(now - g_start_epoch);
        if (uptime < 0) uptime = 0;
        char body[256];
        int blen = snprintf(body, sizeof(body),
            "{\"status\":\"ok\",\"uptime_s\":%ld,\"pid\":%d}\n",
            uptime, (int)getpid());
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n\r\n",
            blen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body, (size_t)blen);
    } else if (route_dash) {
        /* Single-page HTML dashboard.  No external assets; fetches
         * /health + /metrics from the same origin via XHR every 2 s.
         * Renders: (1) cumulative stat cards, (2) derived-rate cards,
         * (3) SVG sparklines for queries/s and rows/s, (4) an events
         * log for auth + flush events, (5) server-identification panel. */
        static const char DASH[] =
"<!DOCTYPE html><html lang=en><head><meta charset=utf-8>"
"<meta name=viewport content=\"width=device-width,initial-scale=1\">"
"<title>QEngine — health</title>"
"<style>"
":root{--fg:#0f172a;--mu:#64748b;--bd:#e2e8f0;--bg:#f8fafc;--card:#fff;"
"--ok:#22c55e;--bad:#ef4444;--warn:#f59e0b;--accent:#2563eb}"
"*{box-sizing:border-box}"
"body{font:14px/1.4 -apple-system,system-ui,sans-serif;color:var(--fg);"
"background:var(--bg);margin:0;padding:24px;max-width:1400px;margin:auto}"
"header{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin-bottom:4px}"
"h1{font-size:20px;margin:0}"
".sub{color:var(--mu);font-size:12px}"
".badge{display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;"
"color:#fff;background:var(--ok);font-weight:500;letter-spacing:.02em}"
".badge.bad{background:var(--bad)}.badge.warn{background:var(--warn)}"
".badge.mode{background:#4338ca;font-weight:600;letter-spacing:.05em}"
".badge.mode.standalone{background:#64748b}"
"section{margin-top:20px}"
".secttl{font-size:11px;color:var(--mu);text-transform:uppercase;letter-spacing:.08em;"
"margin-bottom:8px;font-weight:600}"
".grid{display:grid;gap:10px}"
".grid.cards{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}"
".grid.wide{grid-template-columns:repeat(auto-fit,minmax(340px,1fr))}"
".card{background:var(--card);border:1px solid var(--bd);border-radius:8px;padding:12px 14px}"
".k{color:var(--mu);font-size:10px;text-transform:uppercase;letter-spacing:.05em}"
".v{font-size:22px;font-weight:600;margin-top:2px;font-variant-numeric:tabular-nums}"
".vs{color:var(--mu);font-size:11px;margin-top:2px;font-variant-numeric:tabular-nums}"
".spark{display:block;width:100%;height:60px;margin-top:8px}"
".spark path{fill:none;stroke:var(--accent);stroke-width:1.5}"
".spark .area{fill:var(--accent);fill-opacity:.10;stroke:none}"
".idtbl{width:100%;font-size:12px;border-collapse:collapse}"
".idtbl td{padding:4px 0;color:var(--mu)}"
".idtbl td.val{color:var(--fg);font-variant-numeric:tabular-nums;text-align:right}"
".idtbl thead td{color:var(--mu);font-size:10px;text-transform:uppercase;"
"letter-spacing:.05em;font-weight:600}"
".idtbl tbody td{padding:4px 8px;border-top:1px solid var(--bd)}"
".pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:10px;font-weight:500}"
".pill.alive{background:#dcfce7;color:#166534}"
".pill.suspect{background:#fef3c7;color:#92400e}"
".pill.dead{background:#fee2e2;color:#991b1b}"
".pill.joining{background:#dbeafe;color:#1e40af}"
".log{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;"
"max-height:240px;overflow:auto}"
".log .row{display:flex;gap:8px;padding:4px 0;border-bottom:1px dotted var(--bd)}"
".log .t{color:var(--mu);white-space:nowrap}"
".log .m{flex:1}"
".log .tag{padding:0 6px;border-radius:4px;font-size:10px;text-transform:uppercase}"
".log .tag.auth{background:#dbeafe;color:#1e40af}"
".log .tag.deny{background:#fee2e2;color:#991b1b}"
".log .tag.flush{background:#dcfce7;color:#166534}"
".log .tag.err{background:#fef3c7;color:#92400e}"
"footer{color:var(--mu);font-size:11px;margin-top:28px;padding-top:12px;"
"border-top:1px solid var(--bd)}"
"</style></head><body>"
"<header>"
"<h1>QEngine</h1>"
"<span id=modebadge class=\"badge mode\">…</span>"
"<span id=st class=badge>loading</span>"
"<span class=sub id=host></span>"
"</header>"
"<div class=sub id=uptime></div>"

"<section><div class=secttl>Realtime rates</div>"
"<div class=\"grid wide\">"
"<div class=card>"
"<div class=k>Queries / sec</div>"
"<div class=v id=rq>—</div>"
"<svg class=spark id=sq viewBox=\"0 0 100 20\" preserveAspectRatio=none></svg>"
"</div>"
"<div class=card>"
"<div class=k>Rows written / sec</div>"
"<div class=v id=rw>—</div>"
"<svg class=spark id=sw viewBox=\"0 0 100 20\" preserveAspectRatio=none></svg>"
"</div>"
"</div></section>"

"<section><div class=secttl>Counters</div>"
"<div class=\"grid cards\" id=g></div></section>"

"<section><div class=secttl>Cluster topology</div>"
"<div class=card>"
"<div class=sub id=cmode style=\"margin-bottom:6px\">-</div>"
"<table class=idtbl id=ctbl><thead>"
"<tr><td>id</td><td>addr</td><td>state</td><td>disk</td><td>vn-weight</td>"
"<td class=val>uptime</td></tr></thead>"
"<tbody id=cbody></tbody></table></div></section>"

"<section><div class=secttl>Events (last 50)</div>"
"<div class=card><div class=log id=log></div></div></section>"

"<section><div class=secttl>Server identification</div>"
"<div class=card>"
"<table class=idtbl id=idt><tr><td>host</td><td class=val id=i_host>-</td></tr>"
"<tr><td>pid</td><td class=val id=i_pid>-</td></tr>"
"<tr><td>uptime</td><td class=val id=i_up>-</td></tr>"
"<tr><td>crc32c impl</td><td class=val>hardware-dispatched</td></tr>"
"</table></div></section>"

"<footer>polling /health + /metrics every 2s · last refresh "
"<span id=t>-</span></footer>"

"<script>"
"const CARDS=["
"['qengine_connections_active','Active conns',''],"
"['qengine_queries_total','Queries total',''],"
"['qengine_rows_written_total','Rows written',''],"
"['qengine_bytes_written_total','Bytes written','B'],"
"['qengine_query_errors_total','Query errors',''],"
"['qengine_flushes_total','Flushes',''],"
"['qengine_bloom_skips_total','Bloom skips',''],"
"['qengine_auth_logins_total','Auth logins',''],"
"['qengine_auth_denied_total','Auth denied',''],"
"['qengine_connections_total','Conns lifetime','']"
"];"
"const MAXPTS=60;"
"const state={qHist:[],wHist:[],prev:null,events:[]};"
"function fmt(v,u){if(v>=1e9)return (v/1e9).toFixed(1)+'G'+u;"
"if(v>=1e6)return (v/1e6).toFixed(1)+'M'+u;"
"if(v>=1e3)return (v/1e3).toFixed(1)+'k'+u;"
"return (Math.round(v*10)/10)+u;}"
"function fmtSec(s){if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m '+(s%60)+'s';"
"const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h+'h '+m+'m';}"
"function spark(id,pts){const el=document.getElementById(id);"
"if(pts.length<2){el.innerHTML='';return;}"
"const max=Math.max(...pts,1);const n=pts.length;"
"const d=pts.map((v,i)=>{const x=(i/(n-1))*100;const y=20-(v/max)*18-1;"
"return (i===0?'M':'L')+x+' '+y;}).join(' ');"
"const area=d+' L100 20 L0 20 Z';"
"el.innerHTML='<path class=area d=\"'+area+'\"/><path d=\"'+d+'\"/>';}"
"function pushEv(tag,txt){"
"const t=new Date().toLocaleTimeString();"
"state.events.unshift({t,tag,txt});"
"if(state.events.length>50)state.events.length=50;"
"document.getElementById('log').innerHTML=state.events.map(e=>"
"`<div class=row><span class=t>${e.t}</span>"
"<span class=\"tag ${e.tag}\">${e.tag}</span>"
"<span class=m>${e.txt}</span></div>`).join('');}"
"async function tick(){try{"
" const hr=await fetch('/health');const h=await hr.json();"
" document.getElementById('st').textContent='ok';"
" document.getElementById('st').className='badge';"
" document.getElementById('i_host').textContent=location.host;"
" document.getElementById('i_pid').textContent=h.pid;"
" document.getElementById('i_up').textContent=fmtSec(h.uptime_s);"
" document.getElementById('host').textContent=location.host;"
" document.getElementById('uptime').textContent='uptime '+fmtSec(h.uptime_s);"
" const r=await (await fetch('/metrics')).text();"
" const m={};r.split('\\n').forEach(l=>{"
"  if(l.startsWith('#'))return;const p=l.indexOf(' ');"
"  if(p<0)return;const k=l.substring(0,p);const v=parseFloat(l.substring(p+1));"
"  if(!Number.isNaN(v))m[k]=v;});"
" const now=Date.now();"
" if(state.prev){"
"  const dt=(now-state.prev.ts)/1000;"
"  const dq=((m.qengine_queries_total||0)-(state.prev.m.qengine_queries_total||0))/dt;"
"  const dw=((m.qengine_rows_written_total||0)-(state.prev.m.qengine_rows_written_total||0))/dt;"
"  state.qHist.push(Math.max(0,dq));if(state.qHist.length>MAXPTS)state.qHist.shift();"
"  state.wHist.push(Math.max(0,dw));if(state.wHist.length>MAXPTS)state.wHist.shift();"
"  document.getElementById('rq').textContent=fmt(dq,'/s');"
"  document.getElementById('rw').textContent=fmt(dw,'/s');"
"  spark('sq',state.qHist);spark('sw',state.wHist);"
"  const pm=state.prev.m;"
"  const dAuth=(m.qengine_auth_logins_total||0)-(pm.qengine_auth_logins_total||0);"
"  if(dAuth>0)pushEv('auth',`+${dAuth} login(s)`);"
"  const dDeny=(m.qengine_auth_denied_total||0)-(pm.qengine_auth_denied_total||0);"
"  if(dDeny>0)pushEv('deny',`+${dDeny} auth denial(s)`);"
"  const dFl=(m.qengine_flushes_total||0)-(pm.qengine_flushes_total||0);"
"  if(dFl>0)pushEv('flush',`+${dFl} flush(es)`);"
"  const dErr=(m.qengine_query_errors_total||0)-(pm.qengine_query_errors_total||0);"
"  if(dErr>0)pushEv('err',`+${dErr} query error(s)`);}"
" state.prev={ts:now,m};"
" document.getElementById('g').innerHTML=CARDS.map(c=>{"
"  const v=m[c[0]]??0;"
"  return `<div class=card><div class=k>${c[1]}</div>"
"<div class=v>${fmt(v,c[2])}</div></div>`;}).join('');"
" document.getElementById('t').textContent=new Date().toLocaleTimeString();"
"}catch(e){"
" document.getElementById('st').textContent='unreachable';"
" document.getElementById('st').className='badge bad';}}"
"function fmtBytes(b){if(!b)return '-';"
"const u=['B','K','M','G','T','P'];let i=0,v=b;"
"while(v>=1024&&i<u.length-1){v/=1024;i++;}"
"return (Math.round(v*10)/10)+u[i];}"
"async function tickCluster(){try{"
" const c=await (await fetch('/cluster')).json();"
" const ab=c.autobalance||{};"
" const abByID={};(ab.nodes||[]).forEach(n=>abByID[n.id]=n);"
" const isStandalone=c.mode==='standalone';"
" const aliveCount=(c.nodes||[]).filter(n=>(n.state||'ALIVE')==='ALIVE').length;"
" const totalCount=(c.nodes||[]).length;"
" const mb=document.getElementById('modebadge');"
" if(isStandalone){"
"   mb.textContent='STANDALONE';"
"   mb.className='badge mode standalone';"
" }else{"
"   mb.textContent='CLUSTER · '+aliveCount+'/'+totalCount+' nodes';"
"   mb.className='badge mode';"
" }"
" // cluster-wide writes/sec from gossip: sum of autobalance.nodes[].writes_sec"
" let clusterWrites=0;"
" (ab.nodes||[]).forEach(n=>{clusterWrites+=(n.writes_sec||0);});"
" document.getElementById('cmode').innerHTML="
"   'local_id: '+(c.local_id||0)"
"   +(isStandalone?'':('   ·   cluster write rate: '+clusterWrites+' rows/sec (sum of autobalance)'))"
"   +(ab.ema_writes_sec!=null?('   ·   ema_local: '+ab.ema_writes_sec):'');"
" const rows=(c.nodes||[]).map(n=>{"
"  const s=(n.state||'ALIVE').toLowerCase();"
"  const isLocal=(String(n.id)===String(c.local_id));"
"  const localDisk=(isLocal&&c.local&&c.local.disk)?c.local.disk:(n.disk||{});"
"  const cap=localDisk.total_bytes?(fmtBytes(localDisk.free_bytes||0)+' / '+fmtBytes(localDisk.total_bytes)):(isStandalone?'-':'peer');"
"  const abNode=abByID[n.id]||{};"
"  const vn=localDisk.vn_weight||abNode.vn||'-';"
"  const up=(isLocal&&c.local&&c.local.uptime_s)?fmtSec(c.local.uptime_s):(n.uptime_s?fmtSec(n.uptime_s):'-');"
"  const badge=isLocal?' <span class=pill alive style=\"margin-left:6px\">this</span>':'';"
"  return `<tr><td>${n.id}${badge}</td><td>${n.addr||'-'}</td>"
"<td><span class=\"pill ${s}\">${(n.state||'ALIVE')}</span></td>"
"<td>${cap}</td><td>${vn}</td>"
"<td class=val>${up}</td></tr>`;}).join('');"
" document.getElementById('cbody').innerHTML=rows||'<tr><td colspan=6>no nodes</td></tr>';"
"}catch(e){"
" document.getElementById('cbody').innerHTML="
"   '<tr><td colspan=6 class=sub>/cluster unreachable: '+e.message+'</td></tr>';}}"
"tick();setInterval(tick,2000);"
"tickCluster();setInterval(tickCluster,5000);"
"</script></body></html>";
        size_t body_len = sizeof(DASH) - 1;
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %zu\r\n"
            "Connection: close\r\n\r\n",
            body_len);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, DASH, body_len);
    } else {
        const char *not_found =
            "HTTP/1.1 404 Not Found\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        write_all(fd, not_found, strlen(not_found));
    }

    close(fd);
    return NULL;
}

/* ---- Accept loop --------------------------------------------------------- */

static void *accept_loop(void *arg) {
    struct tsdb_metrics_server *ms = (struct tsdb_metrics_server *)arg;

    while (ms->running) {
        struct pollfd pfd = { ms->listen_fd, POLLIN, 0 };
        int r = poll(&pfd, 1, 200);
        if (r <= 0) continue;
        if (!(pfd.revents & POLLIN)) continue;

        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(ms->listen_fd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }

        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, handle_connection, (void *)(intptr_t)cfd) != 0) {
            close(cfd);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

/* ---- Public API ---------------------------------------------------------- */

static int parse_bind(const char *addr, char *host_out, size_t hcap, int *port_out) {
    if (!addr || addr[0] == '\0') return -1;
    const char *colon = strrchr(addr, ':');
    if (!colon) return -1;
    size_t hlen = (size_t)(colon - addr);
    if (hlen >= hcap) hlen = hcap - 1;
    memcpy(host_out, addr, hlen);
    host_out[hlen] = '\0';
    *port_out = atoi(colon + 1);
    return 0;
}

int tsdb_metrics_server_start(const char *bind_addr, tsdb_metrics_server_t **out) {
    *out = NULL;
    if (!bind_addr || bind_addr[0] == '\0') return 0; /* disabled */

    pthread_once(&g_once, record_start);

    char host[128];
    int  port;
    if (parse_bind(bind_addr, host, sizeof(host), &port) != 0) return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) <= 0) sa.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }
    /* Larger backlog: when several dashboard tabs + Prometheus scrape
     * the same node, connection arrivals burst.  32 was enough for
     * scrapes-only but gets noisy with the embedded dashboard polling
     * /health + /metrics + /cluster every 2–5 s per tab. */
    if (listen(fd, 512) < 0) {
        close(fd);
        return -1;
    }

    /* Get actual port (if port was 0). */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);
    int actual_port = ntohs(bound.sin_port);

    struct tsdb_metrics_server *ms =
        (struct tsdb_metrics_server *)calloc(1, sizeof(*ms));
    if (!ms) { close(fd); return -1; }

    ms->listen_fd = fd;
    ms->port      = actual_port;
    ms->running   = 1;

    if (pthread_create(&ms->accept_thread, NULL, accept_loop, ms) != 0) {
        close(fd);
        free(ms);
        return -1;
    }

    *out = ms;
    return 0;
}

void tsdb_metrics_server_stop(tsdb_metrics_server_t *ms) {
    if (!ms) return;
    ms->running = 0;
    close(ms->listen_fd);
    pthread_join(ms->accept_thread, NULL);
    free(ms);
}

int tsdb_metrics_server_port(const tsdb_metrics_server_t *ms) {
    if (!ms) return -1;
    return ms->port;
}
