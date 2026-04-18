#include "parse.h"
#include "lex.h"
#include "../catalog/group.h"
#include "../catalog/device.h"
#include "../catalog/stable.h"
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

typedef struct {
    qlex_t       lex;
    qtok_t       tok;
    tsdb_arena_t *arena;
    char        *err;
    size_t       errcap;
    int          errored;
} parser_t;

/* Forward decls: definitions appear further down. */
static int ident_ci(const qtok_t *t, const char *s);

static void perr(parser_t *p, const char *fmt, ...) {
    if (p->errored) return;
    p->errored = 1;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(p->err, p->errcap, fmt, ap);
    va_end(ap);
}

static int advance(parser_t *p) {
    int rc = qlex_next(&p->lex, &p->tok);
    if (rc != TSDB_OK) {
        perr(p, "lex error at line %d col %d: %s", p->lex.line, p->lex.col, p->lex.err);
    }
    return rc;
}

static int accept(parser_t *p, qtok_kind_t k) {
    if (p->tok.kind == k) { advance(p); return 1; }
    return 0;
}

static int expect(parser_t *p, qtok_kind_t k) {
    if (p->tok.kind == k) return advance(p);
    perr(p, "expected %s at line %d col %d, got %s", qtok_name(k), p->tok.line, p->tok.col, qtok_name(p->tok.kind));
    return TSDB_ERR_PARSE;
}

/* forward */
static qast_expr_t *parse_expr(parser_t *p);

static char *tok_strdup(parser_t *p, const qtok_t *t) {
    return tsdb_arena_strndup(p->arena, t->start, t->len);
}

/* string literal value — handle '' escape */
static char *str_literal(parser_t *p, const qtok_t *t) {
    char *s = tsdb_arena_alloc(p->arena, t->len + 1);
    if (!s) return NULL;
    size_t j = 0;
    for (size_t i = 0; i < t->len; i++) {
        if (t->start[i] == '\'' && i + 1 < t->len && t->start[i + 1] == '\'') {
            s[j++] = '\''; i++;
        } else s[j++] = t->start[i];
    }
    s[j] = 0;
    return s;
}

static qast_expr_t *parse_primary(parser_t *p) {
    qtok_t t = p->tok;
    if (t.kind == QTOK_NUMBER) {
        advance(p);
        return qast_mk_int(p->arena, t.i);
    }
    if (t.kind == QTOK_FLOAT) {
        advance(p);
        return qast_mk_float(p->arena, t.f);
    }
    if (t.kind == QTOK_INTERVAL) {
        advance(p);
        return qast_mk_int(p->arena, t.i);
    }
    if (t.kind == QTOK_STRING) {
        advance(p);
        char *s = str_literal(p, &t);
        /* Try parse as ISO timestamp; if it parses, treat as TS literal. */
        tsdb_ts_t ts = tsdb_parse_ts(s);
        if (ts != TSDB_TS_MIN) return qast_mk_ts(p->arena, ts);
        qast_expr_t *e = qast_mk_str(p->arena, s);
        return e;
    }
    if (t.kind == QTOK_NULL_KW) {
        advance(p);
        return qast_mk_null(p->arena);
    }
    if (t.kind == QTOK_STAR) {
        advance(p);
        return qast_mk_star(p->arena);
    }
    if (t.kind == QTOK_LPAREN) {
        advance(p);
        qast_expr_t *e = parse_expr(p);
        if (!e) return NULL;
        if (expect(p, QTOK_RPAREN) != TSDB_OK) return NULL;
        return e;
    }
    if (t.kind == QTOK_IDENT) {
        advance(p);
        if (p->tok.kind == QTOK_LPAREN) {
            char *name = tok_strdup(p, &t);
            advance(p);
            qast_expr_t *args[32];
            int nargs = 0;
            if (p->tok.kind != QTOK_RPAREN) {
                for (;;) {
                    if (nargs >= 32) { perr(p, "too many call args"); return NULL; }
                    qast_expr_t *a = parse_expr(p);
                    if (!a) return NULL;
                    args[nargs++] = a;
                    if (!accept(p, QTOK_COMMA)) break;
                }
            }
            if (expect(p, QTOK_RPAREN) != TSDB_OK) return NULL;
            return qast_mk_call(p->arena, name, args, nargs);
        }
        return qast_mk_ident(p->arena, tok_strdup(p, &t));
    }
    perr(p, "unexpected token %s at line %d col %d", qtok_name(t.kind), t.line, t.col);
    return NULL;
}

static qast_expr_t *parse_unary(parser_t *p) {
    if (p->tok.kind == QTOK_MINUS) {
        advance(p);
        qast_expr_t *x = parse_unary(p);
        if (!x) return NULL;
        return qast_mk_unary(p->arena, QAST_NEG, x);
    }
    if (p->tok.kind == QTOK_NOT) {
        advance(p);
        qast_expr_t *x = parse_unary(p);
        if (!x) return NULL;
        return qast_mk_unary(p->arena, QAST_NOT, x);
    }
    return parse_primary(p);
}

static qast_expr_t *parse_mul(parser_t *p) {
    qast_expr_t *l = parse_unary(p);
    while (l && !p->errored) {
        qast_kind_t k;
        if (p->tok.kind == QTOK_STAR) k = QAST_MUL;
        else if (p->tok.kind == QTOK_SLASH) k = QAST_DIV;
        else if (p->tok.kind == QTOK_PERCENT) k = QAST_MOD;
        else break;
        advance(p);
        qast_expr_t *r = parse_unary(p);
        if (!r) return NULL;
        l = qast_mk_binop(p->arena, k, l, r);
    }
    return l;
}

static qast_expr_t *parse_add(parser_t *p) {
    qast_expr_t *l = parse_mul(p);
    while (l && !p->errored) {
        qast_kind_t k;
        if (p->tok.kind == QTOK_PLUS) k = QAST_ADD;
        else if (p->tok.kind == QTOK_MINUS) k = QAST_SUB;
        else break;
        advance(p);
        qast_expr_t *r = parse_mul(p);
        if (!r) return NULL;
        l = qast_mk_binop(p->arena, k, l, r);
    }
    return l;
}

static qast_expr_t *parse_cmp(parser_t *p) {
    qast_expr_t *l = parse_add(p);
    if (!l || p->errored) return l;
    qast_kind_t k;
    switch (p->tok.kind) {
    case QTOK_EQ: k = QAST_EQ; break;
    case QTOK_NE: k = QAST_NE; break;
    case QTOK_LT: k = QAST_LT; break;
    case QTOK_LE: k = QAST_LE; break;
    case QTOK_GT: k = QAST_GT; break;
    case QTOK_GE: k = QAST_GE; break;
    default: return l;
    }
    advance(p);
    qast_expr_t *r = parse_add(p);
    if (!r) return NULL;
    return qast_mk_binop(p->arena, k, l, r);
}

static qast_expr_t *parse_and(parser_t *p) {
    qast_expr_t *l = parse_cmp(p);
    while (l && !p->errored && p->tok.kind == QTOK_AND) {
        advance(p);
        qast_expr_t *r = parse_cmp(p);
        if (!r) return NULL;
        l = qast_mk_binop(p->arena, QAST_AND, l, r);
    }
    return l;
}

static qast_expr_t *parse_or(parser_t *p) {
    qast_expr_t *l = parse_and(p);
    while (l && !p->errored && p->tok.kind == QTOK_OR) {
        advance(p);
        qast_expr_t *r = parse_and(p);
        if (!r) return NULL;
        l = qast_mk_binop(p->arena, QAST_OR, l, r);
    }
    return l;
}

static qast_expr_t *parse_expr(parser_t *p) { return parse_or(p); }

static int parse_sel_list(parser_t *p, qast_query_t *q) {
    qast_sel_item_t items[64];
    int n = 0;
    for (;;) {
        if (n >= 64) { perr(p, "too many select items"); return TSDB_ERR_PARSE; }
        qast_sel_item_t it;
        memset(&it, 0, sizeof(it));
        if (p->tok.kind == QTOK_STAR) {
            advance(p);
            it.is_star = 1;
        } else {
            it.expr = parse_expr(p);
            if (!it.expr) return TSDB_ERR_PARSE;
            if (p->tok.kind == QTOK_AS) {
                advance(p);
                if (p->tok.kind != QTOK_IDENT) { perr(p, "expected ident after AS"); return TSDB_ERR_PARSE; }
                it.alias = tok_strdup(p, &p->tok);
                advance(p);
            }
        }
        items[n++] = it;
        if (!accept(p, QTOK_COMMA)) break;
    }
    q->sel = tsdb_arena_alloc(p->arena, sizeof(qast_sel_item_t) * (size_t)n);
    if (!q->sel) return TSDB_ERR_NOMEM;
    memcpy(q->sel, items, sizeof(qast_sel_item_t) * (size_t)n);
    q->nsel = n;
    return TSDB_OK;
}

static int parse_ident_list(parser_t *p, char ***out_arr, int *out_n) {
    char *names[64];
    int n = 0;
    for (;;) {
        if (p->tok.kind != QTOK_IDENT) { perr(p, "expected identifier"); return TSDB_ERR_PARSE; }
        if (n >= 64) { perr(p, "too many identifiers"); return TSDB_ERR_PARSE; }
        names[n++] = tok_strdup(p, &p->tok);
        advance(p);
        if (!accept(p, QTOK_COMMA)) break;
    }
    char **arr = tsdb_arena_alloc(p->arena, sizeof(char *) * (size_t)n);
    if (!arr) return TSDB_ERR_NOMEM;
    memcpy(arr, names, sizeof(char *) * (size_t)n);
    *out_arr = arr;
    *out_n = n;
    return TSDB_OK;
}

int qparse(const char *src, tsdb_arena_t *a, qast_query_t *q, char *err, size_t errcap) {
    parser_t p;
    qlex_init(&p.lex, src);
    p.arena = a;
    p.err = err;
    p.errcap = errcap;
    p.errored = 0;
    if (err && errcap) err[0] = 0;
    memset(q, 0, sizeof(*q));

    if (advance(&p) != TSDB_OK) return TSDB_ERR_PARSE;

    if (!accept(&p, QTOK_SELECT)) { perr(&p, "expected SELECT"); return TSDB_ERR_PARSE; }
    if (parse_sel_list(&p, q) != TSDB_OK) return TSDB_ERR_PARSE;

    if (expect(&p, QTOK_FROM) != TSDB_OK) return TSDB_ERR_PARSE;
    if (p.tok.kind != QTOK_IDENT) { perr(&p, "expected table name"); return TSDB_ERR_PARSE; }
    q->from = tok_strdup(&p, &p.tok);
    advance(&p);

    /* ASOF JOIN ident ON left_key = right_key [, ...] */
    if (accept(&p, QTOK_ASOF)) {
        if (expect(&p, QTOK_JOIN) != TSDB_OK) return TSDB_ERR_PARSE;
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected right table name after ASOF JOIN");
            return TSDB_ERR_PARSE;
        }
        q->asof_table = tok_strdup(&p, &p.tok);
        advance(&p);
        q->has_asof_join = 1;

        if (accept(&p, QTOK_ON)) {
            char *lkeys[32], *rkeys[32];
            int nk = 0;
            for (;;) {
                if (nk >= 32) { perr(&p, "too many ASOF JOIN ON keys"); return TSDB_ERR_PARSE; }
                if (p.tok.kind != QTOK_IDENT) { perr(&p, "expected column name in ASOF JOIN ON"); return TSDB_ERR_PARSE; }
                lkeys[nk] = tok_strdup(&p, &p.tok);
                advance(&p);
                if (expect(&p, QTOK_EQ) != TSDB_OK) return TSDB_ERR_PARSE;
                if (p.tok.kind != QTOK_IDENT) { perr(&p, "expected column name after '=' in ASOF JOIN ON"); return TSDB_ERR_PARSE; }
                rkeys[nk] = tok_strdup(&p, &p.tok);
                advance(&p);
                nk++;
                if (!accept(&p, QTOK_COMMA)) break;
            }
            q->asof_n_keys = nk;
            q->asof_on_keys_l = tsdb_arena_alloc(p.arena, sizeof(char *) * (size_t)nk);
            q->asof_on_keys_r = tsdb_arena_alloc(p.arena, sizeof(char *) * (size_t)nk);
            if (!q->asof_on_keys_l || !q->asof_on_keys_r) return TSDB_ERR_NOMEM;
            for (int i = 0; i < nk; i++) {
                q->asof_on_keys_l[i] = lkeys[i];
                q->asof_on_keys_r[i] = rkeys[i];
            }
        }
    }

    if (accept(&p, QTOK_WHERE)) {
        q->where = parse_expr(&p);
        if (!q->where) return TSDB_ERR_PARSE;
    }

    /* Top-level PARTITION BY tbname — STable per-child dimension.
     * Placed between WHERE and SAMPLE BY so it does not collide with the
     * PARTITION BY consumed inside the later LATEST ON ... PARTITION BY block. */
    if (p.tok.kind == QTOK_PARTITION) {
        advance(&p);
        if (expect(&p, QTOK_BY) != TSDB_OK) return TSDB_ERR_PARSE;
        if (p.tok.kind != QTOK_IDENT || !ident_ci(&p.tok, "tbname")) {
            perr(&p, "expected 'tbname' after top-level PARTITION BY (v0.9: only tbname supported)");
            return TSDB_ERR_PARSE;
        }
        advance(&p);
        q->has_partition_by_tbname = 1;
    }

    if (accept(&p, QTOK_SAMPLE)) {
        if (expect(&p, QTOK_BY) != TSDB_OK) return TSDB_ERR_PARSE;
        if (p.tok.kind != QTOK_INTERVAL && p.tok.kind != QTOK_NUMBER) {
            perr(&p, "expected interval after SAMPLE BY"); return TSDB_ERR_PARSE;
        }
        q->sample_by.ns = p.tok.i;
        q->has_sample_by = 1;
        advance(&p);

        q->fill.kind = QAST_FILL_NONE;
        if (accept(&p, QTOK_FILL)) {
            if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
            if (accept(&p, QTOK_PREV)) q->fill.kind = QAST_FILL_PREV;
            else if (accept(&p, QTOK_NULL_KW)) q->fill.kind = QAST_FILL_NULL;
            else if (p.tok.kind == QTOK_NUMBER) {
                q->fill.kind = QAST_FILL_CONST;
                q->fill.const_v = (double)p.tok.i;
                advance(&p);
            } else if (p.tok.kind == QTOK_FLOAT) {
                q->fill.kind = QAST_FILL_CONST;
                q->fill.const_v = p.tok.f;
                advance(&p);
            } else { perr(&p, "bad FILL spec"); return TSDB_ERR_PARSE; }
            if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
        }
    }

    /* ---- Advanced window: SESSION / STATE_WINDOW / EVENT_WINDOW ----------- */
    if (accept(&p, QTOK_SESSION)) {
        if (q->has_sample_by) {
            perr(&p, "SESSION cannot be combined with SAMPLE BY");
            return TSDB_ERR_PARSE;
        }
        if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
        /* first arg: ts column name (consumed but ignored — we use schema ts col) */
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "SESSION: expected column name as first argument");
            return TSDB_ERR_PARSE;
        }
        advance(&p);
        if (expect(&p, QTOK_COMMA) != TSDB_OK) return TSDB_ERR_PARSE;
        /* second arg: gap interval */
        if (p.tok.kind != QTOK_INTERVAL && p.tok.kind != QTOK_NUMBER) {
            perr(&p, "SESSION: expected interval as second argument (e.g. 5m, 3s)");
            return TSDB_ERR_PARSE;
        }
        q->session_gap_ns = p.tok.i;
        advance(&p);
        if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
        q->has_adv_window = 1;
        q->adv_window_kind = QAST_WIN_SESSION;
    } else if (accept(&p, QTOK_STATE_WINDOW)) {
        if (q->has_sample_by) {
            perr(&p, "STATE_WINDOW cannot be combined with SAMPLE BY");
            return TSDB_ERR_PARSE;
        }
        if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "STATE_WINDOW: expected state column name");
            return TSDB_ERR_PARSE;
        }
        q->state_col = tok_strdup(&p, &p.tok);
        advance(&p);
        if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
        q->has_adv_window = 1;
        q->adv_window_kind = QAST_WIN_STATE;
    } else if (accept(&p, QTOK_EVENT_WINDOW)) {
        if (q->has_sample_by) {
            perr(&p, "EVENT_WINDOW cannot be combined with SAMPLE BY");
            return TSDB_ERR_PARSE;
        }
        if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
        q->event_start_expr = parse_expr(&p);
        if (!q->event_start_expr) return TSDB_ERR_PARSE;
        if (expect(&p, QTOK_COMMA) != TSDB_OK) return TSDB_ERR_PARSE;
        q->event_end_expr = parse_expr(&p);
        if (!q->event_end_expr) return TSDB_ERR_PARSE;
        if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
        q->has_adv_window = 1;
        q->adv_window_kind = QAST_WIN_EVENT;
    }

    if (accept(&p, QTOK_LATEST)) {
        if (expect(&p, QTOK_ON) != TSDB_OK) return TSDB_ERR_PARSE;
        if (p.tok.kind != QTOK_IDENT) { perr(&p, "expected ts column after LATEST ON"); return TSDB_ERR_PARSE; }
        q->latest_on_col = tok_strdup(&p, &p.tok);
        advance(&p);
        q->has_latest_on = 1;
        if (accept(&p, QTOK_PARTITION)) {
            if (expect(&p, QTOK_BY) != TSDB_OK) return TSDB_ERR_PARSE;
            if (parse_ident_list(&p, &q->latest_part_cols, &q->nlatest_part) != TSDB_OK) return TSDB_ERR_PARSE;
        }
    }

    if (accept(&p, QTOK_GROUP)) {
        if (expect(&p, QTOK_BY) != TSDB_OK) return TSDB_ERR_PARSE;
        if (parse_ident_list(&p, &q->group_by, &q->ngroup_by) != TSDB_OK) return TSDB_ERR_PARSE;
    }

    if (accept(&p, QTOK_ORDER)) {
        if (expect(&p, QTOK_BY) != TSDB_OK) return TSDB_ERR_PARSE;
        if (p.tok.kind != QTOK_IDENT) { perr(&p, "expected identifier after ORDER BY"); return TSDB_ERR_PARSE; }
        q->order_col = tok_strdup(&p, &p.tok);
        advance(&p);
        q->order_dir = QAST_ORDER_ASC;
        if (accept(&p, QTOK_ASC))  q->order_dir = QAST_ORDER_ASC;
        else if (accept(&p, QTOK_DESC)) q->order_dir = QAST_ORDER_DESC;
        q->has_order = 1;
    }

    if (accept(&p, QTOK_LIMIT)) {
        if (p.tok.kind != QTOK_NUMBER) { perr(&p, "expected number after LIMIT"); return TSDB_ERR_PARSE; }
        q->limit = p.tok.i;
        q->has_limit = 1;
        advance(&p);
    }

    accept(&p, QTOK_SEMI);
    if (p.tok.kind != QTOK_EOF) {
        perr(&p, "trailing tokens starting with %s", qtok_name(p.tok.kind));
        return TSDB_ERR_PARSE;
    }
    return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
}

/* ---- DDL helpers -------------------------------------------------------- */

/* Case-insensitive strncmp helper. */
static int ident_ci(const qtok_t *t, const char *s) {
    size_t n = strlen(s);
    if (t->len != n) return 0;
    for (size_t i = 0; i < n; i++) {
        if (tolower((unsigned char)t->start[i]) != tolower((unsigned char)s[i])) return 0;
    }
    return 1;
}

/* Safe copy of token text into fixed buffer. */
static void tok_copy(const qtok_t *t, char *dst, size_t cap) {
    size_t n = t->len < cap - 1 ? t->len : cap - 1;
    memcpy(dst, t->start, n);
    dst[n] = '\0';
}

/* Parse (key=value, ...) property list for CREATE GROUP / CREATE DEVICE.
 * Recognised keys are set on the spec structs; unknown keys are silently
 * accumulated in the 'tags' field as "key=value," pairs. */

static int parse_props_group(parser_t *p, tsdb_group_t *g) {
    /* optional paren-delimited list */
    int has_paren = accept(p, QTOK_LPAREN);
    if (!has_paren && p->tok.kind != QTOK_IDENT &&
        p->tok.kind != QTOK_REGION && p->tok.kind != QTOK_RETENTION &&
        p->tok.kind != QTOK_PROFILE && p->tok.kind != QTOK_REPLICA) {
        return TSDB_OK; /* no props */
    }
    char tags_buf[512];
    tags_buf[0] = '\0';

    while (!p->errored) {
        /* key can be a keyword or ident */
        qtok_kind_t kk = p->tok.kind;
        if (kk != QTOK_IDENT && kk != QTOK_REGION && kk != QTOK_RETENTION &&
            kk != QTOK_PROFILE && kk != QTOK_REPLICA && kk != QTOK_FACTOR) {
            break;
        }
        qtok_t key = p->tok;
        advance(p);

        if (expect(p, QTOK_EQ) != TSDB_OK) return TSDB_ERR_PARSE;

        /* value: string or number */
        char valstr[256];
        if (p->tok.kind == QTOK_STRING) {
            size_t n = p->tok.len < sizeof(valstr) - 1 ? p->tok.len : sizeof(valstr) - 1;
            memcpy(valstr, p->tok.start, n);
            valstr[n] = '\0';
            advance(p);
        } else if (p->tok.kind == QTOK_NUMBER) {
            snprintf(valstr, sizeof(valstr), "%lld", (long long)p->tok.i);
            advance(p);
        } else if (p->tok.kind == QTOK_INTERVAL) {
            snprintf(valstr, sizeof(valstr), "%lld", (long long)p->tok.i);
            advance(p);
        } else {
            perr(p, "expected string or number value in properties"); return TSDB_ERR_PARSE;
        }

        /* Dispatch known keys. */
        if (key.kind == QTOK_REGION || ident_ci(&key, "region")) {
            snprintf(g->region, sizeof(g->region), "%s", valstr);
        } else if (key.kind == QTOK_RETENTION || ident_ci(&key, "retention")) {
            /* value can be "30d" (interval string) or integer ns. */
            int64_t ns = qlex_parse_interval(valstr, strlen(valstr));
            if (ns > 0) g->retention_ns = ns;
            else g->retention_ns = strtoll(valstr, NULL, 10);
        } else if (key.kind == QTOK_PROFILE || ident_ci(&key, "profile")) {
            snprintf(g->codec_profile, sizeof(g->codec_profile), "%s", valstr);
        } else if (key.kind == QTOK_REPLICA || ident_ci(&key, "replica")) {
            g->replica_factor = atoi(valstr);
        } else {
            /* Unknown key → accumulate in tags. */
            char keystr[128];
            tok_copy(&key, keystr, sizeof(keystr));
            size_t tl = strlen(tags_buf);
            snprintf(tags_buf + tl, sizeof(tags_buf) - tl, "%s=%s,", keystr, valstr);
        }

        if (!accept(p, QTOK_COMMA)) break;
        /* allow trailing comma before ) */
        if (has_paren && p->tok.kind == QTOK_RPAREN) break;
    }

    if (has_paren && expect(p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
    /* Strip trailing comma. */
    size_t tl = strlen(tags_buf);
    if (tl > 0 && tags_buf[tl-1] == ',') tags_buf[tl-1] = '\0';
    snprintf(g->tags, sizeof(g->tags), "%s", tags_buf);
    return TSDB_OK;
}

static int parse_props_device(parser_t *p, tsdb_device_t *d) {
    int has_paren = accept(p, QTOK_LPAREN);
    if (!has_paren && p->tok.kind != QTOK_IDENT) return TSDB_OK;

    char tags_buf[512];
    tags_buf[0] = '\0';

    while (!p->errored) {
        if (p->tok.kind != QTOK_IDENT) break;
        qtok_t key = p->tok;
        advance(p);
        if (expect(p, QTOK_EQ) != TSDB_OK) return TSDB_ERR_PARSE;

        char valstr[256];
        if (p->tok.kind == QTOK_STRING) {
            size_t n = p->tok.len < sizeof(valstr) - 1 ? p->tok.len : sizeof(valstr) - 1;
            memcpy(valstr, p->tok.start, n);
            valstr[n] = '\0';
            advance(p);
        } else if (p->tok.kind == QTOK_NUMBER) {
            snprintf(valstr, sizeof(valstr), "%lld", (long long)p->tok.i);
            advance(p);
        } else {
            perr(p, "expected value"); return TSDB_ERR_PARSE;
        }

        if (ident_ci(&key, "type")) {
            snprintf(d->type, sizeof(d->type), "%s", valstr);
        } else if (ident_ci(&key, "location")) {
            snprintf(d->location, sizeof(d->location), "%s", valstr);
        } else if (ident_ci(&key, "status")) {
            d->status = atoi(valstr);
        } else {
            char keystr[128];
            tok_copy(&key, keystr, sizeof(keystr));
            size_t tl = strlen(tags_buf);
            snprintf(tags_buf + tl, sizeof(tags_buf) - tl, "%s=%s,", keystr, valstr);
        }

        if (!accept(p, QTOK_COMMA)) break;
        if (has_paren && p->tok.kind == QTOK_RPAREN) break;
    }

    if (has_paren && expect(p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
    size_t tl = strlen(tags_buf);
    if (tl > 0 && tags_buf[tl-1] == ',') tags_buf[tl-1] = '\0';
    snprintf(d->tags, sizeof(d->tags), "%s", tags_buf);
    return TSDB_OK;
}

/* Return 1 if tok can be used as a column/table name (ident or non-DDL keyword
 * like REGION, GROUP, etc. that might appear in user data). */
static int tok_is_name_like(const qtok_t *t) {
    if (t->kind == QTOK_IDENT) return 1;
    /* Allow keywords that commonly appear in identifiers */
    switch (t->kind) {
    case QTOK_GROUP:     case QTOK_REGION:   case QTOK_ON:
    case QTOK_ORDER:     case QTOK_DESC:     case QTOK_ASC:
    case QTOK_FILL:      case QTOK_PREV:     case QTOK_IN:
    case QTOK_PROFILE:   case QTOK_FACTOR:   case QTOK_LIMIT:
    case QTOK_RETENTION: case QTOK_REPLICA:  case QTOK_PARTITION:
    case QTOK_SAMPLE:    case QTOK_BY:       case QTOK_SESSION:
    case QTOK_JOIN:      case QTOK_LATEST:   case QTOK_DEVICE:
    case QTOK_TRUE:      case QTOK_FALSE:    case QTOK_NULL_KW:
    case QTOK_LIST:
        return 1;
    default:
        return 0;
    }
}

/* ---- qparse_stmt -------------------------------------------------------- */

int qparse_stmt(const char *src, tsdb_arena_t *a, qast_stmt_t *out,
                char *err, size_t errcap) {
    if (!src || !out) return TSDB_ERR_INVAL;

    parser_t p;
    qlex_init(&p.lex, src);
    p.arena = a;
    p.err = err;
    p.errcap = errcap;
    p.errored = 0;
    if (err && errcap) err[0] = 0;
    memset(out, 0, sizeof(*out));

    if (advance(&p) != TSDB_OK) return TSDB_ERR_PARSE;

    /* ---- SELECT (existing query) ---------------------------------------- */
    if (p.tok.kind == QTOK_SELECT) {
        out->kind = QAST_STMT_SELECT;
        /* Delegate to qparse. The existing qparse re-initialises its own
         * parser struct so we can call it directly on the original src. */
        return qparse(src, a, &out->u.query, err, errcap);
    }

    /* ---- CREATE --------------------------------------------------------- */
    if (accept(&p, QTOK_CREATE)) {
        /* CREATE GROUP <name> [(props)] */
        if (accept(&p, QTOK_GROUP)) {
            out->kind = QAST_STMT_CREATE_GROUP;
            tsdb_group_t *g = &out->u.create_group.spec;
            memset(g, 0, sizeof(*g));
            g->replica_factor = 3;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected group name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, g->name, sizeof(g->name));
            advance(&p);
            if (parse_props_group(&p, g) != TSDB_OK) return TSDB_ERR_PARSE;
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* CREATE DEVICE <id> IN GROUP <group> [(props)] */
        if (accept(&p, QTOK_DEVICE)) {
            out->kind = QAST_STMT_CREATE_DEVICE;
            tsdb_device_t *d = &out->u.create_device.spec;
            memset(d, 0, sizeof(*d));

            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected device id"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, d->id, sizeof(d->id));
            advance(&p);

            if (!accept(&p, QTOK_IN)) {
                perr(&p, "expected IN after device id"); return TSDB_ERR_PARSE;
            }
            if (!accept(&p, QTOK_GROUP)) {
                perr(&p, "expected GROUP after IN"); return TSDB_ERR_PARSE;
            }
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected group name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, d->group, sizeof(d->group));
            advance(&p);

            if (parse_props_device(&p, d) != TSDB_OK) return TSDB_ERR_PARSE;
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* CREATE STABLE name (col type, ...) TAGS (tag_col type, ...) */
        if (accept(&p, QTOK_STABLE)) {
            out->kind = QAST_STMT_CREATE_STABLE;
            tsdb_stable_t *st = &out->u.create_stable.spec;
            memset(st, 0, sizeof(*st));
            st->ts_col_idx = -1;

            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected stable name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, st->name, sizeof(st->name));
            advance(&p);

            /* Parse column list: (col_name type, ...) */
            if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
            while (!p.errored && p.tok.kind != QTOK_RPAREN && p.tok.kind != QTOK_EOF) {
                if (st->ncols >= TSDB_STABLE_MAX_COLS) {
                    perr(&p, "too many columns in STABLE"); return TSDB_ERR_PARSE;
                }
                if (!tok_is_name_like(&p.tok)) {
                    perr(&p, "expected column name"); return TSDB_ERR_PARSE;
                }
                tsdb_stable_col_t *col = &st->cols[st->ncols];
                tok_copy(&p.tok, col->name, sizeof(col->name));
                advance(&p);
                /* type keyword: TIMESTAMP, INT64, FLOAT64, SYMBOL — lexed as IDENT */
                if (!tok_is_name_like(&p.tok)) {
                    perr(&p, "expected column type"); return TSDB_ERR_PARSE;
                }
                if (ident_ci(&p.tok, "timestamp")) {
                    col->type = TSDB_TYPE_TIMESTAMP;
                    if (st->ts_col_idx < 0) st->ts_col_idx = st->ncols;
                } else if (ident_ci(&p.tok, "int64")) {
                    col->type = TSDB_TYPE_INT64;
                } else if (ident_ci(&p.tok, "float64")) {
                    col->type = TSDB_TYPE_FLOAT64;
                } else if (ident_ci(&p.tok, "symbol")) {
                    col->type = TSDB_TYPE_SYMBOL;
                } else {
                    perr(&p, "unknown column type '%.*s'", (int)p.tok.len, p.tok.start);
                    return TSDB_ERR_PARSE;
                }
                advance(&p);
                st->ncols++;
                if (!accept(&p, QTOK_COMMA)) break;
                if (p.tok.kind == QTOK_RPAREN) break; /* trailing comma */
            }
            if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
            if (st->ts_col_idx < 0) st->ts_col_idx = 0; /* default to first col */

            /* Parse TAGS (tag_col type, ...) */
            if (p.tok.kind != QTOK_TAGS) {
                perr(&p, "expected TAGS after column list"); return TSDB_ERR_PARSE;
            }
            advance(&p);
            if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
            while (!p.errored && p.tok.kind != QTOK_RPAREN && p.tok.kind != QTOK_EOF) {
                if (st->ntag_cols >= TSDB_STABLE_MAX_TAG_COLS) {
                    perr(&p, "too many tag columns in STABLE"); return TSDB_ERR_PARSE;
                }
                if (!tok_is_name_like(&p.tok)) {
                    perr(&p, "expected tag column name"); return TSDB_ERR_PARSE;
                }
                tsdb_stable_col_t *tc = &st->tag_cols[st->ntag_cols];
                tok_copy(&p.tok, tc->name, sizeof(tc->name));
                advance(&p);
                if (!tok_is_name_like(&p.tok)) {
                    perr(&p, "expected tag column type"); return TSDB_ERR_PARSE;
                }
                if (ident_ci(&p.tok, "int64")) {
                    tc->type = TSDB_TYPE_INT64;
                } else if (ident_ci(&p.tok, "float64")) {
                    tc->type = TSDB_TYPE_FLOAT64;
                } else if (ident_ci(&p.tok, "symbol")) {
                    tc->type = TSDB_TYPE_SYMBOL;
                } else {
                    perr(&p, "unknown tag type '%.*s'", (int)p.tok.len, p.tok.start);
                    return TSDB_ERR_PARSE;
                }
                advance(&p);
                st->ntag_cols++;
                if (!accept(&p, QTOK_COMMA)) break;
                if (p.tok.kind == QTOK_RPAREN) break;
            }
            if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* CREATE TABLE name USING stable_name TAGS (val, val, ...) */
        if (accept(&p, QTOK_TABLE)) {
            out->kind = QAST_STMT_CREATE_CHILD_TABLE;
            tsdb_child_table_t *ct = &out->u.create_child_table.spec;
            memset(ct, 0, sizeof(*ct));

            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected child table name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, ct->name, sizeof(ct->name));
            advance(&p);

            /* USING stable_name */
            if (p.tok.kind != QTOK_USING) {
                perr(&p, "expected USING after child table name"); return TSDB_ERR_PARSE;
            }
            advance(&p);
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected stable name after USING"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, ct->stable_name, sizeof(ct->stable_name));
            advance(&p);

            /* TAGS (val, val, ...) */
            if (p.tok.kind != QTOK_TAGS) {
                perr(&p, "expected TAGS after stable name"); return TSDB_ERR_PARSE;
            }
            advance(&p);
            if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
            while (!p.errored && p.tok.kind != QTOK_RPAREN && p.tok.kind != QTOK_EOF) {
                if (ct->ntags >= TSDB_STABLE_MAX_TAG_COLS) {
                    perr(&p, "too many tag values"); return TSDB_ERR_PARSE;
                }
                tsdb_tag_val_t *tv = &ct->tags[ct->ntags];
                if (p.tok.kind == QTOK_STRING) {
                    tv->type = TSDB_TYPE_SYMBOL;
                    char *sv = str_literal(&p, &p.tok);
                    if (!sv) return TSDB_ERR_NOMEM;
                    size_t slen = strlen(sv);
                    if (slen >= sizeof(tv->v.s)) slen = sizeof(tv->v.s) - 1;
                    memcpy(tv->v.s, sv, slen);
                    tv->v.s[slen] = '\0';
                    advance(&p);
                } else if (p.tok.kind == QTOK_NUMBER) {
                    tv->type = TSDB_TYPE_INT64;
                    tv->v.i  = p.tok.i;
                    advance(&p);
                } else if (p.tok.kind == QTOK_FLOAT) {
                    tv->type = TSDB_TYPE_FLOAT64;
                    tv->v.f  = p.tok.f;
                    advance(&p);
                } else if (p.tok.kind == QTOK_MINUS) {
                    advance(&p);
                    if (p.tok.kind == QTOK_NUMBER) {
                        tv->type = TSDB_TYPE_INT64;
                        tv->v.i  = -p.tok.i;
                        advance(&p);
                    } else if (p.tok.kind == QTOK_FLOAT) {
                        tv->type = TSDB_TYPE_FLOAT64;
                        tv->v.f  = -p.tok.f;
                        advance(&p);
                    } else {
                        perr(&p, "expected number after '-' in TAGS"); return TSDB_ERR_PARSE;
                    }
                } else {
                    perr(&p, "expected tag value (string or number)"); return TSDB_ERR_PARSE;
                }
                ct->ntags++;
                if (!accept(&p, QTOK_COMMA)) break;
                if (p.tok.kind == QTOK_RPAREN) break;
            }
            if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        perr(&p, "expected GROUP, DEVICE, STABLE, or TABLE after CREATE");
        return TSDB_ERR_PARSE;
    }

    /* ---- DROP ----------------------------------------------------------- */
    if (accept(&p, QTOK_DROP)) {
        /* DROP GROUP <name> */
        if (accept(&p, QTOK_GROUP)) {
            out->kind = QAST_STMT_DROP_GROUP;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected group name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_group.name, sizeof(out->u.drop_group.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* DROP DEVICE <id> IN GROUP <group> */
        if (accept(&p, QTOK_DEVICE)) {
            out->kind = QAST_STMT_DROP_DEVICE;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected device id"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_device.id, sizeof(out->u.drop_device.id));
            advance(&p);
            if (!accept(&p, QTOK_IN)) {
                perr(&p, "expected IN"); return TSDB_ERR_PARSE;
            }
            if (!accept(&p, QTOK_GROUP)) {
                perr(&p, "expected GROUP"); return TSDB_ERR_PARSE;
            }
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected group name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_device.group, sizeof(out->u.drop_device.group));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* DROP STABLE name */
        if (accept(&p, QTOK_STABLE)) {
            out->kind = QAST_STMT_DROP_STABLE;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected stable name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_stable.name, sizeof(out->u.drop_stable.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        perr(&p, "expected GROUP, DEVICE, or STABLE after DROP");
        return TSDB_ERR_PARSE;
    }

    /* ---- LIST ----------------------------------------------------------- */
    if (accept(&p, QTOK_LIST)) {
        /* LIST GROUPS  — "groups" lexes as QTOK_IDENT (plural not a keyword) */
        if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "groups")) {
            out->kind = QAST_STMT_LIST_GROUPS;
            advance(&p);
            accept(&p, QTOK_SEMI);
            return TSDB_OK;
        }

        /* LIST DEVICES [IN GROUP <group>] */
        if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "devices")) {
            out->kind = QAST_STMT_LIST_DEVICES;
            out->u.list_devices.group[0] = '\0';
            advance(&p);
            if (accept(&p, QTOK_IN)) {
                if (!accept(&p, QTOK_GROUP)) {
                    perr(&p, "expected GROUP after IN"); return TSDB_ERR_PARSE;
                }
                if (p.tok.kind != QTOK_IDENT) {
                    perr(&p, "expected group name"); return TSDB_ERR_PARSE;
                }
                tok_copy(&p.tok, out->u.list_devices.group,
                         sizeof(out->u.list_devices.group));
                advance(&p);
            }
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        perr(&p, "expected GROUPS or DEVICES after LIST");
        return TSDB_ERR_PARSE;
    }

    perr(&p, "expected SELECT, CREATE, DROP, or LIST");
    return TSDB_ERR_PARSE;
}

/* qparse: legacy SELECT-only wrapper.  Calls qparse_stmt and errors for
 * any non-SELECT statement so callers that only expect SELECT still work. */
/* NOTE: qparse is already defined above this block.  We instead just update
 * tsdb_query in exec.c to call qparse_stmt directly. */
