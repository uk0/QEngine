/* promql.c — PromQL subset translation layer for tsdb.
 *
 * Architecture:
 *   1. Tokenizer  — returns a stream of pql_tok_t
 *   2. Parser     — recursive-descent, ~5 productions, emits QTL directly
 *   3. JSON builder — hand-rolled, no dependencies
 *
 * Supported patterns (see promql.h for full list).
 */

#include "promql.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <ctype.h>
#include <math.h>

/* =========================================================================
 * Tokenizer
 * ========================================================================= */

typedef enum {
    PQL_TOK_EOF = 0,
    PQL_TOK_IDENT,       /* metric name or keyword */
    PQL_TOK_STRING,      /* "value" inside label matchers */
    PQL_TOK_NUMBER,      /* integer or float */
    PQL_TOK_DURATION,    /* e.g. 5m, 1h, 30s — stored as nanoseconds in ival */
    PQL_TOK_LBRACE,      /* { */
    PQL_TOK_RBRACE,      /* } */
    PQL_TOK_LPAREN,      /* ( */
    PQL_TOK_RPAREN,      /* ) */
    PQL_TOK_LBRACKET,    /* [ */
    PQL_TOK_RBRACKET,    /* ] */
    PQL_TOK_COMMA,       /* , */
    PQL_TOK_EQ,          /* = */
    PQL_TOK_NEQ,         /* != */
    PQL_TOK_ERR
} pql_tok_kind_t;

typedef struct {
    pql_tok_kind_t kind;
    const char    *start;  /* pointer into source */
    size_t         len;    /* byte length of token text */
    int64_t        ival;   /* for NUMBER (int) and DURATION (nanoseconds) */
    double         fval;   /* for NUMBER (float) */
    int            is_float;
} pql_tok_t;

typedef struct {
    const char *src;
    const char *cur;
    pql_tok_t   peek;      /* one-token lookahead */
    int         peek_valid;
    char        err[256];
} pql_lex_t;

static void pql_lex_init(pql_lex_t *l, const char *src) {
    l->src = src;
    l->cur = src;
    l->peek_valid = 0;
    l->err[0] = '\0';
}

/* Convert duration suffix to nanoseconds multiplier. */
static int64_t dur_unit_ns(const char *s, size_t n) {
    if (n == 2) {
        if ((s[0]=='n'||s[0]=='N') && (s[1]=='s'||s[1]=='S')) return 1LL;
        if ((s[0]=='u'||s[0]=='U') && (s[1]=='s'||s[1]=='S')) return 1000LL;
        if ((s[0]=='m'||s[0]=='M') && (s[1]=='s'||s[1]=='S')) return 1000000LL;
    }
    if (n == 1) {
        if (s[0]=='s'||s[0]=='S') return 1000000000LL;
        if (s[0]=='m'||s[0]=='M') return 60000000000LL;
        if (s[0]=='h'||s[0]=='H') return 3600000000000LL;
        if (s[0]=='d'||s[0]=='D') return 86400000000000LL;
        if (s[0]=='w'||s[0]=='W') return 604800000000000LL;
        if (s[0]=='y'||s[0]=='Y') return 31536000000000000LL;
    }
    return 0;
}

/* Check if character is valid in a duration unit suffix. */
static int is_dur_unit_char(char c) {
    return c=='n'||c=='N'||c=='u'||c=='U'||c=='m'||c=='M'||c=='s'||c=='S'||
           c=='h'||c=='H'||c=='d'||c=='D'||c=='w'||c=='W'||c=='y'||c=='Y';
}

static void pql_skip_ws(pql_lex_t *l) {
    while (*l->cur && isspace((unsigned char)*l->cur))
        l->cur++;
}

static int pql_lex_next_raw(pql_lex_t *l, pql_tok_t *out) {
    pql_skip_ws(l);
    memset(out, 0, sizeof(*out));
    out->start = l->cur;

    char c = *l->cur;
    if (c == '\0') { out->kind = PQL_TOK_EOF; out->len = 0; return 0; }

    /* Single-char punctuation */
    switch (c) {
    case '{': out->kind = PQL_TOK_LBRACE;   l->cur++; out->len = 1; return 0;
    case '}': out->kind = PQL_TOK_RBRACE;   l->cur++; out->len = 1; return 0;
    case '(': out->kind = PQL_TOK_LPAREN;   l->cur++; out->len = 1; return 0;
    case ')': out->kind = PQL_TOK_RPAREN;   l->cur++; out->len = 1; return 0;
    case '[': out->kind = PQL_TOK_LBRACKET; l->cur++; out->len = 1; return 0;
    case ']': out->kind = PQL_TOK_RBRACKET; l->cur++; out->len = 1; return 0;
    case ',': out->kind = PQL_TOK_COMMA;    l->cur++; out->len = 1; return 0;
    case '=':
        if (l->cur[1] == '~') {
            snprintf(l->err, sizeof(l->err), "promql: regex matchers (=~) unsupported");
            out->kind = PQL_TOK_ERR; return -1;
        }
        out->kind = PQL_TOK_EQ; l->cur++; out->len = 1; return 0;
    case '!':
        if (l->cur[1] == '=') {
            out->kind = PQL_TOK_NEQ; l->cur += 2; out->len = 2; return 0;
        }
        snprintf(l->err, sizeof(l->err), "promql: unexpected '!'");
        out->kind = PQL_TOK_ERR; return -1;
    }

    /* Double-quoted string (label values in PromQL) */
    if (c == '"') {
        l->cur++; /* skip opening " */
        out->start = l->cur;
        while (*l->cur && *l->cur != '"') {
            if (*l->cur == '\\' && l->cur[1]) l->cur++; /* escape */
            l->cur++;
        }
        out->len = (size_t)(l->cur - out->start);
        out->kind = PQL_TOK_STRING;
        if (*l->cur == '"') l->cur++;
        return 0;
    }

    /* Single-quoted string (also allow in label values) */
    if (c == '\'') {
        l->cur++;
        out->start = l->cur;
        while (*l->cur && *l->cur != '\'') {
            if (*l->cur == '\\' && l->cur[1]) l->cur++;
            l->cur++;
        }
        out->len = (size_t)(l->cur - out->start);
        out->kind = PQL_TOK_STRING;
        if (*l->cur == '\'') l->cur++;
        return 0;
    }

    /* Numbers (possibly followed by duration suffix) */
    if (isdigit((unsigned char)c) || (c == '.' && isdigit((unsigned char)l->cur[1]))) {
        const char *s = l->cur;
        int is_float = 0;
        while (isdigit((unsigned char)*l->cur)) l->cur++;
        if (*l->cur == '.') { is_float = 1; l->cur++; while (isdigit((unsigned char)*l->cur)) l->cur++; }

        /* Check for duration suffix immediately after digits */
        size_t ulen = 0;
        const char *ustart = l->cur;
        while (is_dur_unit_char(l->cur[ulen])) ulen++;
        if (ulen > 0 && ulen <= 2) {
            int64_t mult = dur_unit_ns(ustart, ulen);
            if (mult > 0) {
                double num = strtod(s, NULL);
                l->cur += ulen;
                out->start = s;
                out->len   = (size_t)(l->cur - s);
                out->kind  = PQL_TOK_DURATION;
                out->ival  = (int64_t)(num * (double)mult);
                return 0;
            }
        }

        out->start   = s;
        out->len     = (size_t)(l->cur - s);
        out->is_float = is_float;
        if (is_float) {
            out->kind = PQL_TOK_NUMBER;
            out->fval = strtod(s, NULL);
        } else {
            out->kind = PQL_TOK_NUMBER;
            char tmp[64];
            size_t cp = out->len < sizeof(tmp)-1 ? out->len : sizeof(tmp)-1;
            memcpy(tmp, s, cp); tmp[cp] = '\0';
            out->ival = (int64_t)strtoll(tmp, NULL, 10);
        }
        return 0;
    }

    /* Identifiers and keywords */
    if (isalpha((unsigned char)c) || c == '_' || c == ':') {
        out->start = l->cur;
        while (isalnum((unsigned char)*l->cur) || *l->cur == '_' || *l->cur == ':')
            l->cur++;
        out->len  = (size_t)(l->cur - out->start);
        out->kind = PQL_TOK_IDENT;
        return 0;
    }

    snprintf(l->err, sizeof(l->err), "promql: unexpected character '%c'", c);
    out->kind = PQL_TOK_ERR;
    return -1;
}

/* Peek at next token without consuming. */
static pql_tok_t pql_peek(pql_lex_t *l) {
    if (!l->peek_valid) {
        pql_lex_next_raw(l, &l->peek);
        l->peek_valid = 1;
    }
    return l->peek;
}

/* Consume and return current peek. */
static pql_tok_t pql_consume(pql_lex_t *l) {
    pql_tok_t t = pql_peek(l);
    l->peek_valid = 0;
    return t;
}

/* Consume if kind matches; return 1 if consumed. */
static int pql_eat(pql_lex_t *l, pql_tok_kind_t kind) {
    if (pql_peek(l).kind == kind) { pql_consume(l); return 1; }
    return 0;
}

/* =========================================================================
 * Growing string buffer — for QTL output
 * ========================================================================= */

typedef struct {
    char  *buf;
    size_t len;
    size_t cap;
    int    oom;
} pql_sbuf_t;

static void sbuf_init(pql_sbuf_t *s) {
    s->buf = (char *)malloc(256);
    s->len = 0;
    s->cap = s->buf ? 256 : 0;
    s->oom = (s->buf == NULL);
    if (s->buf) s->buf[0] = '\0';
}

static void sbuf_free(pql_sbuf_t *s) {
    free(s->buf);
    s->buf = NULL;
    s->len = 0;
    s->cap = 0;
}

static void sbuf_ensure(pql_sbuf_t *s, size_t extra) {
    if (s->oom) return;
    if (s->len + extra + 1 <= s->cap) return;
    size_t newcap = s->cap * 2;
    while (newcap < s->len + extra + 1) newcap *= 2;
    char *nb = (char *)realloc(s->buf, newcap);
    if (!nb) { s->oom = 1; return; }
    s->buf = nb;
    s->cap = newcap;
}

static void sbuf_append(pql_sbuf_t *s, const char *str, size_t n) {
    sbuf_ensure(s, n);
    if (s->oom) return;
    memcpy(s->buf + s->len, str, n);
    s->len += n;
    s->buf[s->len] = '\0';
}

static void sbuf_appends(pql_sbuf_t *s, const char *str) {
    sbuf_append(s, str, strlen(str));
}

static void sbuf_appendf(pql_sbuf_t *s, const char *fmt, ...) {
    if (s->oom) return;
    char tmp[512];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n > 0) sbuf_append(s, tmp, (size_t)(n < (int)sizeof(tmp) ? n : (int)sizeof(tmp)-1));
}

/* =========================================================================
 * Parser state
 * ========================================================================= */

typedef struct {
    pql_lex_t   lex;
    pql_sbuf_t  qtl;      /* accumulated QTL output */
    char        errbuf[256];
    /* Extracted info for format_result grouping */
    char        metric_name[128];
    char        group_label[128];
    int         has_error;
} pql_parser_t;

static void pql_parser_init(pql_parser_t *p, const char *src) {
    pql_lex_init(&p->lex, src);
    sbuf_init(&p->qtl);
    p->errbuf[0]     = '\0';
    p->metric_name[0]= '\0';
    p->group_label[0]= '\0';
    p->has_error     = 0;
}

static void pql_error(pql_parser_t *p, const char *msg) {
    if (!p->has_error) {
        snprintf(p->errbuf, sizeof(p->errbuf), "%s", msg);
        p->has_error = 1;
    }
}

/* Copy token text into a fixed buffer, NUL-terminated. */
static void tok_str(const pql_tok_t *t, char *buf, size_t cap) {
    size_t n = t->len < cap - 1 ? t->len : cap - 1;
    memcpy(buf, t->start, n);
    buf[n] = '\0';
}

/* =========================================================================
 * Helpers to escape single-quoted SQL string values
 * ========================================================================= */

/* Append val (length vlen) to qtl as a single-quoted SQL string.
 * Doubles any embedded single quotes. */
static void sbuf_append_sql_str(pql_sbuf_t *s, const char *val, size_t vlen) {
    sbuf_appends(s, "'");
    for (size_t i = 0; i < vlen; i++) {
        if (val[i] == '\'') sbuf_appends(s, "''");
        else sbuf_append(s, val + i, 1);
    }
    sbuf_appends(s, "'");
}

/* =========================================================================
 * Parse label selector block: {k="v"[,k2="v2"...]}
 * Emits:  WHERE k='v' [AND k2='v2' ...]
 * Returns number of labels parsed, or -1 on error.
 * ========================================================================= */
static int parse_label_selectors(pql_parser_t *p, int first_clause) {
    /* caller already consumed '{' */
    int count = 0;
    while (1) {
        pql_tok_t t = pql_peek(&p->lex);
        if (t.kind == PQL_TOK_RBRACE) { pql_consume(&p->lex); break; }
        if (t.kind == PQL_TOK_EOF) {
            pql_error(p, "promql: unterminated label selector");
            return -1;
        }
        if (t.kind == PQL_TOK_COMMA) { pql_consume(&p->lex); continue; }
        if (t.kind != PQL_TOK_IDENT) {
            pql_error(p, "promql: expected label name");
            return -1;
        }
        char key[128];
        tok_str(&t, key, sizeof(key));
        pql_consume(&p->lex);

        /* Expect = or != */
        pql_tok_t op = pql_consume(&p->lex);
        int negate = 0;
        if (op.kind == PQL_TOK_ERR) {
            /* Propagate lex error (e.g. =~ unsupported) */
            if (p->lex.err[0]) pql_error(p, p->lex.err);
            else                pql_error(p, "promql: lex error in label selector");
            return -1;
        }
        if (op.kind == PQL_TOK_NEQ) negate = 1;
        else if (op.kind != PQL_TOK_EQ) {
            pql_error(p, "promql: expected '=' or '!=' in label selector");
            return -1;
        }

        pql_tok_t v = pql_consume(&p->lex);
        if (v.kind != PQL_TOK_STRING) {
            pql_error(p, "promql: expected quoted string for label value");
            return -1;
        }

        /* Emit WHERE / AND clause */
        if (first_clause && count == 0) sbuf_appends(&p->qtl, " WHERE ");
        else sbuf_appends(&p->qtl, " AND ");

        sbuf_appends(&p->qtl, key);
        if (negate) sbuf_appends(&p->qtl, " != ");
        else        sbuf_appends(&p->qtl, " = ");
        sbuf_append_sql_str(&p->qtl, v.start, v.len);
        count++;
    }
    return count;
}

/* =========================================================================
 * Duration extraction helper
 * Parses [Xd] inside a range vector selector.
 * Returns ns, or 0 on error.
 * ========================================================================= */
static int64_t parse_range(pql_parser_t *p) {
    /* caller already consumed '[' */
    pql_tok_t t = pql_consume(&p->lex);
    if (t.kind != PQL_TOK_DURATION) {
        pql_error(p, "promql: expected duration inside []");
        return 0;
    }
    int64_t ns = t.ival;
    if (!pql_eat(&p->lex, PQL_TOK_RBRACKET)) {
        pql_error(p, "promql: expected ']' after duration");
        return 0;
    }
    return ns;
}

/* Convert nanoseconds to the shortest QTL duration string that QTL accepts.
 * QTL units: ns, us, ms, s, m, h, d.
 * We produce the unit that results in a whole-integer value. */
static void ns_to_qtl_dur(int64_t ns, char *buf, size_t cap) {
    if (ns <= 0) { snprintf(buf, cap, "1s"); return; }
    /* Try from largest to smallest to pick the most readable unit */
    int64_t units[][2] = {
        {86400000000000LL, 'd'},
        {3600000000000LL,  'h'},
        {60000000000LL,    'm'},
        {1000000000LL,     's'},
        {1000000LL,         0 },  /* ms — two chars */
        {1000LL,            0 },  /* us */
        {1LL,               0 },  /* ns */
    };
    const char *sufx[] = { "d","h","m","s","ms","us","ns" };
    for (int i = 0; i < 7; i++) {
        if (ns % units[i][0] == 0) {
            snprintf(buf, cap, "%lld%s", (long long)(ns / units[i][0]), sufx[i]);
            return;
        }
    }
    /* Fallback: express in nanoseconds */
    snprintf(buf, cap, "%lldns", (long long)ns);
}

static void parse_expr(pql_parser_t *p) {
    pql_tok_t t = pql_peek(&p->lex);

    if (t.kind != PQL_TOK_IDENT) {
        pql_error(p, "promql: expected identifier or function name");
        return;
    }
    pql_consume(&p->lex);

    char name[128];
    tok_str(&t, name, sizeof(name));

    /* ---- Function calls ---- */

    /* rate(metric[Xd]) */
    if (strcasecmp(name, "rate") == 0) {
        if (!pql_eat(&p->lex, PQL_TOK_LPAREN)) {
            pql_error(p, "promql: expected '(' after rate");
            return;
        }
        /* NOTE: rate() semantically divides by window; we approximate with
         * derivative() which computes instantaneous slope. The window
         * duration is parsed but currently unused in QTL since derivative()
         * cannot be combined with SAMPLE BY in QTL. */
        char metric[128] = "";
        int64_t range_ns = 0;
        pql_tok_t mt = pql_peek(&p->lex);
        if (mt.kind != PQL_TOK_IDENT) {
            pql_error(p, "promql: expected metric name inside rate()");
            return;
        }
        pql_consume(&p->lex);
        tok_str(&mt, metric, sizeof(metric));

        /* Optional labels */
        pql_sbuf_t where_frag;
        sbuf_init(&where_frag);
        if (pql_peek(&p->lex).kind == PQL_TOK_LBRACE) {
            pql_consume(&p->lex);
            pql_sbuf_t saved = p->qtl;
            p->qtl = where_frag;
            int n = parse_label_selectors(p, 1);
            where_frag = p->qtl;
            p->qtl = saved;
            if (n < 0) { sbuf_free(&where_frag); return; }
        }

        /* Range */
        if (pql_peek(&p->lex).kind == PQL_TOK_LBRACKET) {
            pql_consume(&p->lex);
            range_ns = parse_range(p);
            if (p->has_error) { sbuf_free(&where_frag); return; }
        }
        if (!pql_eat(&p->lex, PQL_TOK_RPAREN)) {
            pql_error(p, "promql: expected ')' to close rate()");
            sbuf_free(&where_frag);
            return;
        }

        snprintf(p->metric_name, sizeof(p->metric_name), "%s", metric);
        sbuf_appends(&p->qtl, "SELECT ts, derivative(value) FROM ");
        sbuf_appends(&p->qtl, metric);
        if (where_frag.len > 0) sbuf_append(&p->qtl, where_frag.buf, where_frag.len);
        sbuf_appends(&p->qtl, " ORDER BY ts");
        sbuf_free(&where_frag);
        (void)range_ns; /* window documented above */
        return;
    }

    /* irate(metric[Xd]) — same as rate for our purposes */
    if (strcasecmp(name, "irate") == 0) {
        if (!pql_eat(&p->lex, PQL_TOK_LPAREN)) {
            pql_error(p, "promql: expected '(' after irate");
            return;
        }
        char metric[128] = "";
        int64_t range_ns = 0;
        pql_tok_t mt = pql_peek(&p->lex);
        if (mt.kind != PQL_TOK_IDENT) { pql_error(p, "promql: expected metric inside irate()"); return; }
        pql_consume(&p->lex);
        tok_str(&mt, metric, sizeof(metric));
        if (pql_peek(&p->lex).kind == PQL_TOK_LBRACKET) {
            pql_consume(&p->lex);
            range_ns = parse_range(p);
            if (p->has_error) return;
        }
        if (!pql_eat(&p->lex, PQL_TOK_RPAREN)) { pql_error(p, "promql: expected ')' to close irate()"); return; }
        snprintf(p->metric_name, sizeof(p->metric_name), "%s", metric);
        sbuf_appends(&p->qtl, "SELECT ts, derivative(value) FROM ");
        sbuf_appends(&p->qtl, metric);
        sbuf_appends(&p->qtl, " ORDER BY ts");
        (void)range_ns;
        return;
    }

    /* avg_over_time(metric[Xd]) */
    if (strcasecmp(name, "avg_over_time") == 0) {
        if (!pql_eat(&p->lex, PQL_TOK_LPAREN)) {
            pql_error(p, "promql: expected '(' after avg_over_time");
            return;
        }
        char metric[128] = "";
        int64_t range_ns = 0;
        pql_tok_t mt = pql_peek(&p->lex);
        if (mt.kind != PQL_TOK_IDENT) { pql_error(p, "promql: expected metric inside avg_over_time()"); return; }
        pql_consume(&p->lex);
        tok_str(&mt, metric, sizeof(metric));

        pql_sbuf_t where_frag;
        sbuf_init(&where_frag);
        if (pql_peek(&p->lex).kind == PQL_TOK_LBRACE) {
            pql_consume(&p->lex);
            pql_sbuf_t saved = p->qtl;
            p->qtl = where_frag;
            int n = parse_label_selectors(p, 1);
            where_frag = p->qtl;
            p->qtl = saved;
            if (n < 0) { sbuf_free(&where_frag); return; }
        }
        if (pql_peek(&p->lex).kind == PQL_TOK_LBRACKET) {
            pql_consume(&p->lex);
            range_ns = parse_range(p);
            if (p->has_error) { sbuf_free(&where_frag); return; }
        }
        if (!pql_eat(&p->lex, PQL_TOK_RPAREN)) {
            pql_error(p, "promql: expected ')' to close avg_over_time()");
            sbuf_free(&where_frag);
            return;
        }
        snprintf(p->metric_name, sizeof(p->metric_name), "%s", metric);
        sbuf_appends(&p->qtl, "SELECT ts, avg(value) FROM ");
        sbuf_appends(&p->qtl, metric);
        if (where_frag.len > 0) sbuf_append(&p->qtl, where_frag.buf, where_frag.len);
        if (range_ns > 0) {
            char dur[32];
            ns_to_qtl_dur(range_ns, dur, sizeof(dur));
            sbuf_appendf(&p->qtl, " SAMPLE BY %s", dur);
        } else {
            sbuf_appends(&p->qtl, " ORDER BY ts");
        }
        sbuf_free(&where_frag);
        return;
    }

    /* sum(metric) or sum by (label) (metric) */
    if (strcasecmp(name, "sum") == 0 ||
        strcasecmp(name, "avg") == 0 ||
        strcasecmp(name, "count") == 0 ||
        strcasecmp(name, "min") == 0 ||
        strcasecmp(name, "max") == 0)
    {
        char agg[16];
        snprintf(agg, sizeof(agg), "%s", name);
        char group_col[128] = "";

        /* Optional "by (label)" before '(' */
        pql_tok_t next = pql_peek(&p->lex);
        if (next.kind == PQL_TOK_IDENT) {
            char kw[16];
            tok_str(&next, kw, sizeof(kw));
            if (strcasecmp(kw, "by") == 0) {
                pql_consume(&p->lex); /* consume "by" */
                if (!pql_eat(&p->lex, PQL_TOK_LPAREN)) {
                    pql_error(p, "promql: expected '(' after 'by'");
                    return;
                }
                pql_tok_t lbl = pql_consume(&p->lex);
                if (lbl.kind != PQL_TOK_IDENT) {
                    pql_error(p, "promql: expected label name after 'by ('");
                    return;
                }
                tok_str(&lbl, group_col, sizeof(group_col));
                if (!pql_eat(&p->lex, PQL_TOK_RPAREN)) {
                    pql_error(p, "promql: expected ')' to close 'by (...)'");
                    return;
                }
            }
        }

        if (!pql_eat(&p->lex, PQL_TOK_LPAREN)) {
            pql_error(p, "promql: expected '(' after aggregation function");
            return;
        }

        /* Inner: could be another function call like rate(...) or a bare metric */
        pql_tok_t inner_tok = pql_peek(&p->lex);
        if (inner_tok.kind != PQL_TOK_IDENT) {
            pql_error(p, "promql: expected metric name inside aggregation");
            return;
        }
        char inner_name[128];
        tok_str(&inner_tok, inner_name, sizeof(inner_name));

        /* Check if inner is a function */
        int inner_is_func = (strcasecmp(inner_name, "rate") == 0 ||
                             strcasecmp(inner_name, "irate") == 0 ||
                             strcasecmp(inner_name, "avg_over_time") == 0);

        if (inner_is_func) {
            /* sum(rate(metric[Xd])) — we only support derivative wrapper */
            /* Emit: SELECT agg(derivative(value)) FROM metric ORDER BY ts */
            /* But QTL doesn't support nested aggregation over window funcs.
             * Degrade: SELECT ts, derivative(value) FROM metric ORDER BY ts */
            pql_consume(&p->lex); /* consume inner func name */
            if (!pql_eat(&p->lex, PQL_TOK_LPAREN)) {
                pql_error(p, "promql: unsupported nested expression");
                return;
            }
            pql_tok_t mt = pql_peek(&p->lex);
            if (mt.kind != PQL_TOK_IDENT) {
                pql_error(p, "promql: expected metric inside nested function");
                return;
            }
            pql_consume(&p->lex);
            char metric[128];
            tok_str(&mt, metric, sizeof(metric));

            pql_sbuf_t where_frag;
            sbuf_init(&where_frag);
            if (pql_peek(&p->lex).kind == PQL_TOK_LBRACE) {
                pql_consume(&p->lex);
                pql_sbuf_t saved = p->qtl;
                p->qtl = where_frag;
                int n = parse_label_selectors(p, 1);
                where_frag = p->qtl;
                p->qtl = saved;
                if (n < 0) { sbuf_free(&where_frag); return; }
            }
            if (pql_peek(&p->lex).kind == PQL_TOK_LBRACKET) {
                pql_consume(&p->lex);
                int64_t r = parse_range(p);
                if (p->has_error) { sbuf_free(&where_frag); return; }
                (void)r;
            }
            if (!pql_eat(&p->lex, PQL_TOK_RPAREN)) { /* close inner func */ }
            if (!pql_eat(&p->lex, PQL_TOK_RPAREN)) { /* close outer agg  */ }

            snprintf(p->metric_name, sizeof(p->metric_name), "%s", metric);
            sbuf_appends(&p->qtl, "SELECT ts, derivative(value) FROM ");
            sbuf_appends(&p->qtl, metric);
            if (where_frag.len > 0) sbuf_append(&p->qtl, where_frag.buf, where_frag.len);
            sbuf_appends(&p->qtl, " ORDER BY ts");
            sbuf_free(&where_frag);
            return;
        }

        /* Inner is a bare metric */
        pql_consume(&p->lex);
        char metric[128];
        snprintf(metric, sizeof(metric), "%s", inner_name);

        /* Build select list */
        char select_list[256];
        if (group_col[0]) {
            if (strcasecmp(agg, "count") == 0)
                snprintf(select_list, sizeof(select_list), "%s, count(*)", group_col);
            else
                snprintf(select_list, sizeof(select_list), "%s, %s(value)", group_col, agg);
        } else {
            if (strcasecmp(agg, "count") == 0)
                snprintf(select_list, sizeof(select_list), "count(*)");
            else
                snprintf(select_list, sizeof(select_list), "%s(value)", agg);
        }

        /* Collect WHERE fragment */
        pql_sbuf_t where_frag;
        sbuf_init(&where_frag);
        if (pql_peek(&p->lex).kind == PQL_TOK_LBRACE) {
            pql_consume(&p->lex);
            pql_sbuf_t saved = p->qtl;
            p->qtl = where_frag;
            int n = parse_label_selectors(p, 1);
            where_frag = p->qtl;
            p->qtl = saved;
            if (n < 0) { sbuf_free(&where_frag); return; }
        }

        /* Optional range */
        if (pql_peek(&p->lex).kind == PQL_TOK_LBRACKET) {
            pql_consume(&p->lex);
            int64_t r = parse_range(p);
            if (p->has_error) { sbuf_free(&where_frag); return; }
            (void)r;
        }
        if (!pql_eat(&p->lex, PQL_TOK_RPAREN)) {
            pql_error(p, "promql: expected ')' to close aggregation");
            sbuf_free(&where_frag);
            return;
        }

        snprintf(p->metric_name, sizeof(p->metric_name), "%s", metric);
        if (group_col[0])
            snprintf(p->group_label, sizeof(p->group_label), "%s", group_col);

        sbuf_appends(&p->qtl, "SELECT ");
        sbuf_appends(&p->qtl, select_list);
        sbuf_appends(&p->qtl, " FROM ");
        sbuf_appends(&p->qtl, metric);
        if (where_frag.len > 0) sbuf_append(&p->qtl, where_frag.buf, where_frag.len);
        if (group_col[0]) sbuf_appendf(&p->qtl, " GROUP BY %s", group_col);
        sbuf_free(&where_frag);
        return;
    }

    /* Bare metric (no function wrapper) — name is the metric */
    snprintf(p->metric_name, sizeof(p->metric_name), "%s", name);
    sbuf_appends(&p->qtl, "SELECT ts, value FROM ");
    sbuf_appends(&p->qtl, name);

    /* Optional label selectors */
    if (pql_peek(&p->lex).kind == PQL_TOK_LBRACE) {
        pql_consume(&p->lex);
        int n = parse_label_selectors(p, 1);
        if (n < 0) return;
    }

    sbuf_appends(&p->qtl, " ORDER BY ts");
}

/* =========================================================================
 * Public API: tsdb_promql_compile
 * ========================================================================= */

char *tsdb_promql_compile(const char *promql,
                          tsdb_ts_t start_ns,
                          tsdb_ts_t end_ns,
                          int64_t step_ns,
                          char *errbuf, size_t errcap)
{
    (void)start_ns; (void)end_ns; (void)step_ns; /* reserved */

    if (!promql) {
        if (errbuf && errcap > 0)
            snprintf(errbuf, errcap, "promql: NULL expression");
        return NULL;
    }

    pql_parser_t p;
    pql_parser_init(&p, promql);

    parse_expr(&p);

    /* Check for lex errors */
    if (p.lex.err[0] && !p.has_error) {
        snprintf(p.errbuf, sizeof(p.errbuf), "%s", p.lex.err);
        p.has_error = 1;
    }

    /* Ensure trailing EOF */
    if (!p.has_error) {
        pql_tok_t last = pql_peek(&p.lex);
        if (last.kind != PQL_TOK_EOF) {
            snprintf(p.errbuf, sizeof(p.errbuf),
                     "promql: unexpected trailing tokens after expression");
            p.has_error = 1;
        }
    }

    if (p.has_error || p.qtl.oom) {
        if (errbuf && errcap > 0) {
            if (p.qtl.oom)
                snprintf(errbuf, errcap, "promql: out of memory");
            else
                snprintf(errbuf, errcap, "%s", p.errbuf);
        }
        sbuf_free(&p.qtl);
        return NULL;
    }

    char *result = p.qtl.buf;
    /* Transfer ownership — do not free p.qtl.buf */
    p.qtl.buf = NULL;
    return result;
}

/* =========================================================================
 * JSON builder helpers
 * ========================================================================= */

/* Append a JSON-escaped string (without surrounding quotes). */
static void json_escape_str(pql_sbuf_t *j, const char *s, size_t n) {
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '"')  { sbuf_appends(j, "\\\""); continue; }
        if (c == '\\') { sbuf_appends(j, "\\\\"); continue; }
        if (c == '\n') { sbuf_appends(j, "\\n");  continue; }
        if (c == '\r') { sbuf_appends(j, "\\r");  continue; }
        if (c == '\t') { sbuf_appends(j, "\\t");  continue; }
        if (c < 0x20) {
            char esc[8];
            snprintf(esc, sizeof(esc), "\\u%04x", c);
            sbuf_appends(j, esc);
            continue;
        }
        sbuf_append(j, s + i, 1);
    }
}

static void json_str(pql_sbuf_t *j, const char *s) {
    sbuf_appends(j, "\"");
    if (s) json_escape_str(j, s, strlen(s));
    sbuf_appends(j, "\"");
}

/* =========================================================================
 * Series grouping for format_result
 *
 * We collect all rows, then group by series key (group_label column value
 * when present, otherwise single series). This avoids requiring pre-sorted
 * input or multiple passes.
 * ========================================================================= */

#define SERIES_MAX    1024
#define VALUES_INIT   64

typedef struct {
    char   key[256];  /* group label value, or "" for ungrouped */
    /* Dynamic array of (timestamp_sec, value_str) pairs */
    double *ts_sec;   /* Prometheus timestamps are float seconds */
    char  **vals;
    size_t  nv;
    size_t  cap;
    int     oom;
} prom_series_t;

static int series_append(prom_series_t *s, double ts_sec, const char *val_str) {
    if (s->nv >= s->cap) {
        size_t newcap = s->cap ? s->cap * 2 : VALUES_INIT;
        double *nts = (double *)realloc(s->ts_sec, newcap * sizeof(double));
        char  **nvs = (char  **)realloc(s->vals,   newcap * sizeof(char *));
        if (!nts || !nvs) { s->oom = 1; free(nts); free(nvs); return -1; }
        s->ts_sec = nts;
        s->vals   = nvs;
        s->cap    = newcap;
    }
    s->ts_sec[s->nv] = ts_sec;
    s->vals[s->nv]   = strdup(val_str ? val_str : "null");
    if (!s->vals[s->nv]) { s->oom = 1; return -1; }
    s->nv++;
    return 0;
}

static void series_free(prom_series_t *s) {
    for (size_t i = 0; i < s->nv; i++) free(s->vals[i]);
    free(s->ts_sec);
    free(s->vals);
    memset(s, 0, sizeof(*s));
}

/* Find or create a series with the given key. */
static prom_series_t *find_or_create_series(prom_series_t *series, int *nseries,
                                             const char *key)
{
    for (int i = 0; i < *nseries; i++) {
        if (strcmp(series[i].key, key) == 0) return &series[i];
    }
    if (*nseries >= SERIES_MAX) return NULL;
    prom_series_t *s = &series[(*nseries)++];
    memset(s, 0, sizeof(*s));
    snprintf(s->key, sizeof(s->key), "%s", key);
    return s;
}

/* =========================================================================
 * Public API: tsdb_promql_format_result
 * ========================================================================= */

char *tsdb_promql_format_result(tsdb_db_t *db,
                                const char *qtl,
                                const char *metric_name,
                                const char *group_label,
                                size_t *out_len)
{
    if (!db || !qtl || !metric_name) {
        if (out_len) *out_len = 0;
        return NULL;
    }

    tsdb_result_t *r = NULL;
    if (tsdb_query(db, qtl, &r) != TSDB_OK || !r) {
        if (out_len) *out_len = 0;
        return NULL;
    }

    int ncols = tsdb_result_ncols(r);

    /* Find column indices */
    int ts_col    = -1;
    int val_col   = -1;
    int group_col = -1;

    for (int i = 0; i < ncols; i++) {
        const char *cname = tsdb_result_col_name(r, i);
        if (!cname) continue;
        tsdb_type_t ctype = tsdb_result_col_type(r, i);
        if (ctype == TSDB_TYPE_TIMESTAMP && ts_col < 0) ts_col = i;
        else if (group_label && strcmp(cname, group_label) == 0) group_col = i;
        else if (val_col < 0) val_col = i;
    }

    /* Allocate series array on heap to avoid large stack allocation */
    prom_series_t *series = (prom_series_t *)calloc(SERIES_MAX, sizeof(prom_series_t));
    if (!series) {
        tsdb_result_free(r);
        if (out_len) *out_len = 0;
        return NULL;
    }
    int nseries = 0;

    /* Collect rows into series */
    char val_buf[64];
    while (tsdb_result_next(r)) {
        /* Determine series key */
        const char *key = "";
        char key_buf[256] = "";
        if (group_col >= 0) {
            tsdb_type_t gt = tsdb_result_col_type(r, group_col);
            if (gt == TSDB_TYPE_SYMBOL) {
                const char *sv = tsdb_result_sym(r, group_col);
                snprintf(key_buf, sizeof(key_buf), "%s", sv ? sv : "");
            } else {
                int64_t iv = tsdb_result_i64(r, group_col);
                snprintf(key_buf, sizeof(key_buf), "%lld", (long long)iv);
            }
            key = key_buf;
        }

        prom_series_t *s = find_or_create_series(series, &nseries, key);
        if (!s) break;

        /* Timestamp */
        double ts_sec = 0.0;
        if (ts_col >= 0 && !tsdb_result_is_null(r, ts_col)) {
            tsdb_ts_t ts_ns = tsdb_result_ts(r, ts_col);
            ts_sec = (double)ts_ns * 1e-9;
        }

        /* Value */
        if (val_col >= 0 && !tsdb_result_is_null(r, val_col)) {
            tsdb_type_t vt = tsdb_result_col_type(r, val_col);
            if (vt == TSDB_TYPE_FLOAT64) {
                double fv = tsdb_result_f64(r, val_col);
                if (isnan(fv))        snprintf(val_buf, sizeof(val_buf), "NaN");
                else if (isinf(fv))   snprintf(val_buf, sizeof(val_buf), fv > 0 ? "+Inf" : "-Inf");
                else                  snprintf(val_buf, sizeof(val_buf), "%.17g", fv);
            } else if (vt == TSDB_TYPE_INT64) {
                int64_t iv = tsdb_result_i64(r, val_col);
                snprintf(val_buf, sizeof(val_buf), "%lld", (long long)iv);
            } else {
                const char *sv = tsdb_result_sym(r, val_col);
                snprintf(val_buf, sizeof(val_buf), "%s", sv ? sv : "");
            }
        } else {
            snprintf(val_buf, sizeof(val_buf), "null");
        }

        series_append(s, ts_sec, val_buf);
    }
    tsdb_result_free(r);

    /* If no rows were returned, produce a single empty series */
    if (nseries == 0) {
        prom_series_t *s = find_or_create_series(series, &nseries, "");
        (void)s;
    }

    /* Build JSON */
    pql_sbuf_t j;
    sbuf_init(&j);

    sbuf_appends(&j, "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[");

    for (int si = 0; si < nseries; si++) {
        prom_series_t *s = &series[si];
        if (si > 0) sbuf_appends(&j, ",");

        /* {"metric":{...},"values":[[ts,"val"],...]} */
        sbuf_appends(&j, "{\"metric\":{");

        /* __name__ */
        sbuf_appends(&j, "\"__name__\":");
        json_str(&j, metric_name);

        /* group label */
        if (group_label && group_label[0] && s->key[0]) {
            sbuf_appends(&j, ",");
            json_str(&j, group_label);
            sbuf_appends(&j, ":");
            json_str(&j, s->key);
        }

        sbuf_appends(&j, "},\"values\":[");

        for (size_t vi = 0; vi < s->nv; vi++) {
            if (vi > 0) sbuf_appends(&j, ",");
            /* [timestamp_float, "value_string"] */
            char ts_buf[32];
            /* Prometheus uses float seconds with up to 3 decimal places */
            snprintf(ts_buf, sizeof(ts_buf), "%.3f", s->ts_sec[vi]);
            sbuf_appends(&j, "[");
            sbuf_appends(&j, ts_buf);
            sbuf_appends(&j, ",");
            json_str(&j, s->vals[vi]);
            sbuf_appends(&j, "]");
        }

        sbuf_appends(&j, "]}");
    }

    sbuf_appends(&j, "]}}");

    /* Cleanup series */
    for (int si = 0; si < nseries; si++) series_free(&series[si]);
    free(series);

    if (j.oom) {
        sbuf_free(&j);
        if (out_len) *out_len = 0;
        return NULL;
    }

    if (out_len) *out_len = j.len;
    char *result = j.buf;
    j.buf = NULL;
    return result;
}
