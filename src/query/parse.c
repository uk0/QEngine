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

    /* ---- ADD MASTER / REMOVE MASTER -----------------------------------
     *
     * ADD is an existing keyword (QTOK_ADD, used by ALTER TABLE ADD
     * COLUMN); REMOVE is a plain identifier.  We accept both here and
     * disambiguate ADD via "ADD MASTER" — since ALTER TABLE is
     * triggered by QTOK_ALTER earlier, a bare ADD at statement start
     * can only be membership change. */
    if (p.tok.kind == QTOK_ADD ||
        (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "remove"))) {
        int is_add = (p.tok.kind == QTOK_ADD);
        advance(&p);
        if (p.tok.kind != QTOK_IDENT || !ident_ci(&p.tok, "master")) {
            perr(&p, "expected MASTER after ADD/REMOVE");
            return TSDB_ERR_PARSE;
        }
        advance(&p);
        /* Target can be:
         *   - quoted string          'host:port' or '12345'
         *   - bare ident (hostname)
         *   - decimal number         (node_id)
         *   - ip-style literal split by lexer — allow fall-through. */
        char target[96] = {0};
        if (p.tok.kind == QTOK_STRING || p.tok.kind == QTOK_IDENT ||
            p.tok.kind == QTOK_NUMBER) {
            tok_copy(&p.tok, target, sizeof(target));
        } else {
            perr(&p, "expected target after MASTER");
            return TSDB_ERR_PARSE;
        }
        advance(&p);
        accept(&p, QTOK_SEMI);
        if (is_add) {
            out->kind = QAST_STMT_ADD_MASTER;
            snprintf(out->u.add_master.target,
                     sizeof(out->u.add_master.target), "%s", target);
        } else {
            out->kind = QAST_STMT_REMOVE_MASTER;
            snprintf(out->u.remove_master.target,
                     sizeof(out->u.remove_master.target), "%s", target);
        }
        return TSDB_OK;
    }

    /* ---- CREATE --------------------------------------------------------- */
    if (accept(&p, QTOK_CREATE)) {
        /* CREATE CONSUMER GROUP <name> ON <topic>  (TMQ) — matched first
         * because GROUP alone is already a valid legacy DDL keyword.       */
        if (accept(&p, QTOK_CONSUMER)) {
            if (!accept(&p, QTOK_GROUP)) {
                perr(&p, "expected GROUP after CONSUMER");
                return TSDB_ERR_PARSE;
            }
            out->kind = QAST_STMT_CREATE_CONSUMER_GROUP;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected consumer-group name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.create_consumer_group.name,
                     sizeof(out->u.create_consumer_group.name));
            advance(&p);
            if (!accept(&p, QTOK_ON)) {
                perr(&p, "expected ON <topic>"); return TSDB_ERR_PARSE;
            }
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected topic name after ON"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.create_consumer_group.topic,
                     sizeof(out->u.create_consumer_group.topic));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* CREATE DATABASE <name> — top-of-hierarchy entity.
         * Accepts either the DATABASE keyword (QTOK_DATABASE if tokenised)
         * or the IDENT "database" / "db" — lex doesn't reserve them. */
        if ((p.tok.kind == QTOK_IDENT &&
             (ident_ci(&p.tok, "database") || ident_ci(&p.tok, "db")))) {
            advance(&p);
            out->kind = QAST_STMT_CREATE_DATABASE;
            memset(&out->u.create_database, 0, sizeof(out->u.create_database));
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected database name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.create_database.name,
                     sizeof(out->u.create_database.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* CREATE GROUP <name> [IN DATABASE <db>] [(props)] */
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
            /* Optional "IN DATABASE <name>" clause. */
            if (accept(&p, QTOK_IN)) {
                if (p.tok.kind != QTOK_IDENT ||
                    !(ident_ci(&p.tok, "database") || ident_ci(&p.tok, "db"))) {
                    perr(&p, "expected DATABASE after IN");
                    return TSDB_ERR_PARSE;
                }
                advance(&p);
                if (p.tok.kind != QTOK_IDENT) {
                    perr(&p, "expected database name"); return TSDB_ERR_PARSE;
                }
                tok_copy(&p.tok, g->database, sizeof(g->database));
                advance(&p);
            }
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

        /* CREATE {STABLE|VTABLE} name (col type, ...) TAGS (tag_col type, ...)
         * VTABLE is the industrial-digital-twin alias for STABLE under the
         * 3-level model (Database → VTable → PTable).  Both forms produce
         * the same AST; downstream code is unchanged. */
        if (accept(&p, QTOK_STABLE) ||
            (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "vtable")
             && (advance(&p), 1))) {
            out->kind = QAST_STMT_CREATE_STABLE;
            tsdb_stable_t *st = &out->u.create_stable.spec;
            memset(st, 0, sizeof(*st));
            st->ts_col_idx = -1;

            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected stable name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, st->name, sizeof(st->name));
            advance(&p);

            /* Optional "IN DATABASE <name>" and/or "IN GROUP <name>"
             * clauses — industrial 4-level hierarchy
             * (Database → Group → VTable → PTable).  Either clause may
             * appear in either order; both are optional. */
            for (int clause = 0; clause < 2 && accept(&p, QTOK_IN); clause++) {
                if (p.tok.kind == QTOK_IDENT &&
                    (ident_ci(&p.tok, "database") || ident_ci(&p.tok, "db"))) {
                    advance(&p);
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected database name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, st->database, sizeof(st->database));
                    advance(&p);
                } else if (p.tok.kind == QTOK_IDENT &&
                           ident_ci(&p.tok, "group")) {
                    advance(&p);
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected group name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, st->group, sizeof(st->group));
                    advance(&p);
                } else if (accept(&p, QTOK_GROUP)) {
                    /* GROUP is a keyword — handle lexer path. */
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected group name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, st->group, sizeof(st->group));
                    advance(&p);
                } else {
                    perr(&p, "expected DATABASE or GROUP after IN");
                    return TSDB_ERR_PARSE;
                }
            }

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
            /* Optional trailing placement clauses — accept BOTH
             *   IN DATABASE prod IN GROUP gw
             * (the strict, two-IN form) AND
             *   IN DATABASE prod GROUP gw
             * (the bare-second-clause form some users expect from the
             * "IN DB X GROUP Y" hierarchical mental model).  Each
             * iteration consumes ONE clause; a leading IN is optional
             * once we've seen the first one. */
            for (int clause = 0; clause < 2; clause++) {
                int saw_in = accept(&p, QTOK_IN);
                if (!saw_in && clause == 0) break;
                if (p.tok.kind == QTOK_IDENT &&
                    (ident_ci(&p.tok, "database") || ident_ci(&p.tok, "db"))) {
                    advance(&p);
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected database name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, st->database, sizeof(st->database));
                    advance(&p);
                } else if (p.tok.kind == QTOK_IDENT &&
                           ident_ci(&p.tok, "group")) {
                    advance(&p);
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected group name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, st->group, sizeof(st->group));
                    advance(&p);
                } else if (accept(&p, QTOK_GROUP)) {
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected group name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, st->group, sizeof(st->group));
                    advance(&p);
                } else if (saw_in) {
                    perr(&p, "expected DATABASE or GROUP after IN");
                    return TSDB_ERR_PARSE;
                }
            }
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* CREATE TABLE — two shapes:
         *   (a) CREATE TABLE name USING stable TAGS(...)          — child table
         *   (b) CREATE TABLE name (col TYPE, ...) TIMESTAMP(ts)
         *       [WITH (BLOCK_POINTS=N, PARTITION='hour'|'day')]   — normal table
         * Shape is decided by whether the token after the name is USING or '('. */
        if (accept(&p, QTOK_TABLE)) {
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected table name"); return TSDB_ERR_PARSE;
            }
            char tbl_name[64];
            tok_copy(&p.tok, tbl_name, sizeof(tbl_name));
            advance(&p);

            /* ----- Shape (b): normal table with column list --------------- */
            if (p.tok.kind == QTOK_LPAREN) {
                out->kind = QAST_STMT_CREATE_TABLE;
                memset(&out->u.create_table, 0, sizeof(out->u.create_table));
                snprintf(out->u.create_table.name,
                         sizeof(out->u.create_table.name), "%s", tbl_name);
                advance(&p);  /* consume '(' */
                int nc = 0;
                while (!p.errored && p.tok.kind != QTOK_RPAREN && p.tok.kind != QTOK_EOF) {
                    if (nc >= TSDB_STABLE_MAX_COLS) {
                        perr(&p, "too many columns"); return TSDB_ERR_PARSE;
                    }
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected column name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, out->u.create_table.col_names[nc],
                             sizeof(out->u.create_table.col_names[nc]));
                    advance(&p);
                    /* Column type — TIMESTAMP, INT64, FLOAT64, SYMBOL */
                    tsdb_type_t ty = 0;
                    if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "timestamp")) { ty = TSDB_TYPE_TIMESTAMP; advance(&p); }
                    else if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "int64"))   { ty = TSDB_TYPE_INT64;   advance(&p); }
                    else if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "float64")) { ty = TSDB_TYPE_FLOAT64; advance(&p); }
                    else if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "symbol"))  { ty = TSDB_TYPE_SYMBOL;  advance(&p); }
                    else {
                        perr(&p, "expected column type (TIMESTAMP/INT64/FLOAT64/SYMBOL)");
                        return TSDB_ERR_PARSE;
                    }
                    out->u.create_table.col_types[nc] = ty;
                    nc++;
                    if (!accept(&p, QTOK_COMMA)) break;
                }
                if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
                out->u.create_table.ncols = nc;

                /* TIMESTAMP(ts_col) designator */
                if (!(p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "timestamp"))) {
                    perr(&p, "expected TIMESTAMP(ts_col) after column list"); return TSDB_ERR_PARSE;
                }
                advance(&p);
                if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
                if (p.tok.kind != QTOK_IDENT) {
                    perr(&p, "expected timestamp column name"); return TSDB_ERR_PARSE;
                }
                tok_copy(&p.tok, out->u.create_table.ts_col,
                         sizeof(out->u.create_table.ts_col));
                advance(&p);
                if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;

                /* Optional WITH (...) clause */
                if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "with")) {
                    advance(&p);
                    if (expect(&p, QTOK_LPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
                    while (!p.errored && p.tok.kind != QTOK_RPAREN && p.tok.kind != QTOK_EOF) {
                        /* "PARTITION" is a reserved keyword (QTOK_PARTITION); accept
                         * it here as the option name without the IDENT-only guard. */
                        int is_bp   = (p.tok.kind == QTOK_IDENT &&
                                       ident_ci(&p.tok, "block_points"));
                        int is_part = (p.tok.kind == QTOK_PARTITION) ||
                                      (p.tok.kind == QTOK_IDENT &&
                                       ident_ci(&p.tok, "partition"));
                        if (!is_bp && !is_part) {
                            perr(&p, "unknown option (expected BLOCK_POINTS or PARTITION)");
                            return TSDB_ERR_PARSE;
                        }
                        advance(&p);
                        if (expect(&p, QTOK_EQ) != TSDB_OK) return TSDB_ERR_PARSE;
                        if (is_bp) {
                            if (p.tok.kind != QTOK_NUMBER) {
                                perr(&p, "BLOCK_POINTS expects integer"); return TSDB_ERR_PARSE;
                            }
                            out->u.create_table.block_points = (int)p.tok.i;
                            advance(&p);
                        } else {
                            if (p.tok.kind != QTOK_STRING) {
                                perr(&p, "PARTITION expects 'hour' or 'day'"); return TSDB_ERR_PARSE;
                            }
                            char *s = str_literal(&p, &p.tok);
                            if (!s) return TSDB_ERR_NOMEM;
                            if (!strcasecmp(s, "hour"))      out->u.create_table.partition_hour = 1;
                            else if (!strcasecmp(s, "day"))  out->u.create_table.partition_hour = 0;
                            else {
                                perr(&p, "PARTITION expects 'hour' or 'day'"); return TSDB_ERR_PARSE;
                            }
                            advance(&p);
                        }
                        if (!accept(&p, QTOK_COMMA)) break;
                    }
                    if (expect(&p, QTOK_RPAREN) != TSDB_OK) return TSDB_ERR_PARSE;
                }

                accept(&p, QTOK_SEMI);
                return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
            }

            /* ----- Shape (a): child table via USING ------------------------ */
            out->kind = QAST_STMT_CREATE_CHILD_TABLE;
            tsdb_child_table_t *ct = &out->u.create_child_table.spec;
            memset(ct, 0, sizeof(*ct));
            snprintf(ct->name, sizeof(ct->name), "%s", tbl_name);

            /* USING stable_name */
            if (p.tok.kind != QTOK_USING) {
                perr(&p, "expected USING or '(' after table name"); return TSDB_ERR_PARSE;
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

        /* CREATE USER <name> IDENTIFIED BY '<password>' [ROLE admin|normal] */
        if (accept(&p, QTOK_USER)) {
            out->kind = QAST_STMT_CREATE_USER;
            memset(&out->u.create_user, 0, sizeof(out->u.create_user));
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected user name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.create_user.name,
                     sizeof(out->u.create_user.name));
            advance(&p);
            if (!accept(&p, QTOK_IDENTIFIED)) {
                perr(&p, "expected IDENTIFIED BY '<password>'");
                return TSDB_ERR_PARSE;
            }
            if (!accept(&p, QTOK_BY)) {
                perr(&p, "expected BY after IDENTIFIED");
                return TSDB_ERR_PARSE;
            }
            if (p.tok.kind != QTOK_STRING) {
                perr(&p, "expected quoted password");
                return TSDB_ERR_PARSE;
            }
            {
                char *pw = str_literal(&p, &p.tok);
                if (!pw) return TSDB_ERR_NOMEM;
                size_t n = strlen(pw);
                if (n >= sizeof(out->u.create_user.password))
                    n = sizeof(out->u.create_user.password) - 1;
                memcpy(out->u.create_user.password, pw, n);
                out->u.create_user.password[n] = '\0';
            }
            advance(&p);
            /* Default role: NORMAL. Optional ROLE keyword. */
            out->u.create_user.role = 0;
            if (accept(&p, QTOK_ROLE)) {
                if (accept(&p, QTOK_ADMIN))       out->u.create_user.role = 1;
                else if (accept(&p, QTOK_NORMAL)) out->u.create_user.role = 0;
                else {
                    perr(&p, "expected ADMIN or NORMAL after ROLE");
                    return TSDB_ERR_PARSE;
                }
            }
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* CREATE FUNCTION f([T[, T...]]) RETURNS T FROM '<.so>' SYMBOL '<sym>' */
        if (accept(&p, QTOK_FUNCTION)) {
            out->kind = QAST_STMT_CREATE_FUNCTION;
            memset(&out->u.create_function, 0, sizeof(out->u.create_function));
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected function name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.create_function.name,
                     sizeof(out->u.create_function.name));
            advance(&p);

            if (!accept(&p, QTOK_LPAREN)) {
                perr(&p, "expected '(' after function name"); return TSDB_ERR_PARSE;
            }
            int n = 0;
            if (p.tok.kind != QTOK_RPAREN) {
                for (;;) {
                    if (n >= 8) {
                        perr(&p, "too many UDF arguments (max 8)"); return TSDB_ERR_PARSE;
                    }
                    if (!tok_is_name_like(&p.tok)) {
                        perr(&p, "expected type name"); return TSDB_ERR_PARSE;
                    }
                    tsdb_type_t t;
                    if      (ident_ci(&p.tok, "timestamp")) t = TSDB_TYPE_TIMESTAMP;
                    else if (ident_ci(&p.tok, "int64"))     t = TSDB_TYPE_INT64;
                    else if (ident_ci(&p.tok, "float64"))   t = TSDB_TYPE_FLOAT64;
                    else {
                        perr(&p, "unknown UDF arg type '%.*s' (v1: TIMESTAMP | INT64 | FLOAT64)",
                             (int)p.tok.len, p.tok.start);
                        return TSDB_ERR_PARSE;
                    }
                    out->u.create_function.arg_types[n++] = t;
                    advance(&p);
                    if (!accept(&p, QTOK_COMMA)) break;
                }
            }
            if (!accept(&p, QTOK_RPAREN)) {
                perr(&p, "expected ')' after argument types"); return TSDB_ERR_PARSE;
            }
            out->u.create_function.nargs = n;

            if (!accept(&p, QTOK_RETURNS)) {
                perr(&p, "expected RETURNS after argument list"); return TSDB_ERR_PARSE;
            }
            {
                if (!tok_is_name_like(&p.tok)) {
                    perr(&p, "expected return type"); return TSDB_ERR_PARSE;
                }
                tsdb_type_t rt;
                if      (ident_ci(&p.tok, "timestamp")) rt = TSDB_TYPE_TIMESTAMP;
                else if (ident_ci(&p.tok, "int64"))     rt = TSDB_TYPE_INT64;
                else if (ident_ci(&p.tok, "float64"))   rt = TSDB_TYPE_FLOAT64;
                else {
                    perr(&p, "unknown UDF return type '%.*s'",
                         (int)p.tok.len, p.tok.start);
                    return TSDB_ERR_PARSE;
                }
                out->u.create_function.ret_type = rt;
                advance(&p);
            }

            if (!accept(&p, QTOK_FROM)) {
                perr(&p, "expected FROM '<.so path>'"); return TSDB_ERR_PARSE;
            }
            if (p.tok.kind != QTOK_STRING) {
                perr(&p, "expected quoted .so path"); return TSDB_ERR_PARSE;
            }
            {
                char *s = str_literal(&p, &p.tok);
                if (!s) return TSDB_ERR_NOMEM;
                size_t n2 = strlen(s);
                if (n2 >= sizeof(out->u.create_function.so_path))
                    n2 = sizeof(out->u.create_function.so_path) - 1;
                memcpy(out->u.create_function.so_path, s, n2);
                out->u.create_function.so_path[n2] = '\0';
            }
            advance(&p);

            /* SYMBOL '<exported_symbol>' — optional; defaults to function name.
             * SYMBOL lexes as IDENT (there is no QTOK_SYMBOL keyword). */
            if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "symbol")) {
                advance(&p);
                if (p.tok.kind != QTOK_STRING) {
                    perr(&p, "expected quoted symbol name"); return TSDB_ERR_PARSE;
                }
                char *s = str_literal(&p, &p.tok);
                if (!s) return TSDB_ERR_NOMEM;
                size_t n2 = strlen(s);
                if (n2 >= sizeof(out->u.create_function.symbol))
                    n2 = sizeof(out->u.create_function.symbol) - 1;
                memcpy(out->u.create_function.symbol, s, n2);
                out->u.create_function.symbol[n2] = '\0';
                advance(&p);
            } else {
                /* Default: symbol == function name */
                snprintf(out->u.create_function.symbol,
                         sizeof(out->u.create_function.symbol),
                         "%s", out->u.create_function.name);
            }

            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        perr(&p, "expected GROUP, DEVICE, STABLE, TABLE, USER, or FUNCTION after CREATE");
        return TSDB_ERR_PARSE;
    }

    /* ---- ALTER (TABLE ... ADD COLUMN | USER ... SET PASSWORD) ----------- */
    if (accept(&p, QTOK_ALTER)) {
        /* ALTER USER <name> SET PASSWORD '<pw>' */
        if (accept(&p, QTOK_USER)) {
            out->kind = QAST_STMT_ALTER_USER_PASSWORD;
            memset(&out->u.alter_user_password, 0, sizeof(out->u.alter_user_password));
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected user name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.alter_user_password.name,
                     sizeof(out->u.alter_user_password.name));
            advance(&p);
            /* SET is lexed as QTOK_IDENT (not a reserved keyword) — accept by ident_ci */
            if (!(p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "set"))) {
                perr(&p, "expected SET after user name"); return TSDB_ERR_PARSE;
            }
            advance(&p);
            if (!accept(&p, QTOK_PASSWORD)) {
                perr(&p, "expected PASSWORD after SET"); return TSDB_ERR_PARSE;
            }
            if (p.tok.kind != QTOK_STRING) {
                perr(&p, "expected quoted password"); return TSDB_ERR_PARSE;
            }
            {
                char *pw = str_literal(&p, &p.tok);
                if (!pw) return TSDB_ERR_NOMEM;
                size_t n = strlen(pw);
                if (n >= sizeof(out->u.alter_user_password.password))
                    n = sizeof(out->u.alter_user_password.password) - 1;
                memcpy(out->u.alter_user_password.password, pw, n);
                out->u.alter_user_password.password[n] = '\0';
            }
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        if (!accept(&p, QTOK_TABLE)) {
            perr(&p, "expected TABLE or USER after ALTER"); return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected table name"); return TSDB_ERR_PARSE;
        }
        out->kind = QAST_STMT_ALTER_ADD_COLUMN;
        tok_copy(&p.tok, out->u.alter_add_column.table,
                 sizeof(out->u.alter_add_column.table));
        advance(&p);

        if (!accept(&p, QTOK_ADD)) {
            perr(&p, "expected ADD after table name"); return TSDB_ERR_PARSE;
        }
        /* COLUMN is optional syntactic sugar — TDengine tolerates both. */
        accept(&p, QTOK_COLUMN);

        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected column name"); return TSDB_ERR_PARSE;
        }
        tok_copy(&p.tok, out->u.alter_add_column.col_name,
                 sizeof(out->u.alter_add_column.col_name));
        advance(&p);

        if (!tok_is_name_like(&p.tok)) {
            perr(&p, "expected column type"); return TSDB_ERR_PARSE;
        }
        if      (ident_ci(&p.tok, "timestamp")) out->u.alter_add_column.col_type = TSDB_TYPE_TIMESTAMP;
        else if (ident_ci(&p.tok, "int64"))     out->u.alter_add_column.col_type = TSDB_TYPE_INT64;
        else if (ident_ci(&p.tok, "float64"))   out->u.alter_add_column.col_type = TSDB_TYPE_FLOAT64;
        else if (ident_ci(&p.tok, "symbol"))    out->u.alter_add_column.col_type = TSDB_TYPE_SYMBOL;
        else { perr(&p, "unknown column type '%.*s'", (int)p.tok.len, p.tok.start);
               return TSDB_ERR_PARSE; }
        advance(&p);

        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    /* ---- DROP ----------------------------------------------------------- */
    if (accept(&p, QTOK_DROP)) {
        /* DROP DATABASE <name> */
        if (p.tok.kind == QTOK_IDENT &&
            (ident_ci(&p.tok, "database") || ident_ci(&p.tok, "db"))) {
            advance(&p);
            out->kind = QAST_STMT_DROP_DATABASE;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected database name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_database.name,
                     sizeof(out->u.drop_database.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }
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

        /* DROP {STABLE|VTABLE} name */
        if (accept(&p, QTOK_STABLE) ||
            (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "vtable")
             && (advance(&p), 1))) {
            out->kind = QAST_STMT_DROP_STABLE;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected vtable name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_stable.name, sizeof(out->u.drop_stable.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* DROP TABLE name — normal table or ptable.  Cascades to catalog
         * child-table entry if the name is registered there.  Also accept
         * PTABLE as a synonym so the dashboard's data-dropptbl button can
         * call it explicitly. */
        if (accept(&p, QTOK_TABLE) ||
            (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "ptable")
             && (advance(&p), 1))) {
            out->kind = QAST_STMT_DROP_TABLE;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected table name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_table.name, sizeof(out->u.drop_table.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* DROP USER <name> */
        if (accept(&p, QTOK_USER)) {
            out->kind = QAST_STMT_DROP_USER;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected user name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_user.name, sizeof(out->u.drop_user.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* DROP FUNCTION <name> */
        if (accept(&p, QTOK_FUNCTION)) {
            out->kind = QAST_STMT_DROP_FUNCTION;
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected function name"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.drop_function.name,
                     sizeof(out->u.drop_function.name));
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        perr(&p, "expected TABLE, GROUP, DEVICE, STABLE, USER, or FUNCTION after DROP");
        return TSDB_ERR_PARSE;
    }

    /* ---- TRUNCATE TABLE <name> ------------------------------------------ */
    if (accept(&p, QTOK_TRUNCATE)) {
        if (!accept(&p, QTOK_TABLE)) {
            perr(&p, "expected TABLE after TRUNCATE");
            return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected table name"); return TSDB_ERR_PARSE;
        }
        out->kind = QAST_STMT_TRUNCATE_TABLE;
        tok_copy(&p.tok, out->u.truncate_table.name,
                 sizeof(out->u.truncate_table.name));
        advance(&p);
        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    /* ---- DELETE FROM <name> WHERE <col> <op> <literal> ------------------- */
    if (accept(&p, QTOK_DELETE)) {
        if (!accept(&p, QTOK_FROM)) {
            perr(&p, "expected FROM after DELETE");
            return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected table name"); return TSDB_ERR_PARSE;
        }
        out->kind = QAST_STMT_DELETE_RANGE;
        tok_copy(&p.tok, out->u.delete_range.table,
                 sizeof(out->u.delete_range.table));
        advance(&p);

        if (!accept(&p, QTOK_WHERE)) {
            perr(&p, "DELETE requires WHERE <ts_col> <op> <cutoff>");
            return TSDB_ERR_PARSE;
        }
        /* Column name — accepted but not used: we only support the table's
         * timestamp column.  Keeping the identifier in the grammar makes
         * predicates read naturally. */
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected ts column name in WHERE"); return TSDB_ERR_PARSE;
        }
        advance(&p);

        /* Comparison operator. */
        int op_lt = -1, inclusive = 0;
        if      (accept(&p, QTOK_LT)) { op_lt = 1; inclusive = 0; }
        else if (accept(&p, QTOK_LE)) { op_lt = 1; inclusive = 1; }
        else if (accept(&p, QTOK_GT)) { op_lt = 0; inclusive = 0; }
        else if (accept(&p, QTOK_GE)) { op_lt = 0; inclusive = 1; }
        else {
            perr(&p, "expected <, <=, >, or >=");
            return TSDB_ERR_PARSE;
        }

        /* Cutoff literal: accept raw ns NUMBER, INTERVAL, or ISO string. */
        int64_t cutoff_ns = 0;
        if (p.tok.kind == QTOK_NUMBER || p.tok.kind == QTOK_INTERVAL) {
            cutoff_ns = p.tok.i;
            advance(&p);
        } else if (p.tok.kind == QTOK_STRING) {
            char buf[128] = {0};
            size_t n = p.tok.len < sizeof(buf) - 1 ? p.tok.len : sizeof(buf) - 1;
            memcpy(buf, p.tok.start, n);
            buf[n] = '\0';
            tsdb_ts_t ts = tsdb_parse_ts(buf);
            if (ts == TSDB_TS_MIN) {
                perr(&p, "cutoff string is not a valid timestamp");
                return TSDB_ERR_PARSE;
            }
            cutoff_ns = (int64_t)ts;
            advance(&p);
        } else {
            perr(&p, "expected timestamp literal after comparison");
            return TSDB_ERR_PARSE;
        }
        out->u.delete_range.cutoff_ns = cutoff_ns;
        out->u.delete_range.op_lt     = (int8_t)op_lt;
        out->u.delete_range.inclusive = (int8_t)inclusive;
        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    /* ---- LIST ----------------------------------------------------------- */
    if (accept(&p, QTOK_LIST)) {
        /* LIST DATABASES */
        if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "databases")) {
            out->kind = QAST_STMT_LIST_DATABASES;
            advance(&p);
            accept(&p, QTOK_SEMI);
            return TSDB_OK;
        }
        /* LIST GROUPS  — "groups" lexes as QTOK_IDENT (plural not a keyword) */
        if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "groups")) {
            out->kind = QAST_STMT_LIST_GROUPS;
            advance(&p);
            accept(&p, QTOK_SEMI);
            return TSDB_OK;
        }

        /* LIST VTABLES [IN DATABASE <db>] [IN GROUP <grp>]
         * Alias: LIST STABLES.  Both scoping clauses are optional and
         * may appear in either order; each restricts the returned
         * VTables to that database / group. */
        if (p.tok.kind == QTOK_IDENT &&
            (ident_ci(&p.tok, "vtables") || ident_ci(&p.tok, "stables"))) {
            out->kind = QAST_STMT_LIST_VTABLES;
            out->u.list_vtables.database[0] = '\0';
            out->u.list_vtables.group[0]    = '\0';
            advance(&p);
            for (int c = 0; c < 2 && accept(&p, QTOK_IN); c++) {
                if (p.tok.kind == QTOK_IDENT &&
                    (ident_ci(&p.tok, "database") || ident_ci(&p.tok, "db"))) {
                    advance(&p);
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected database name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, out->u.list_vtables.database,
                             sizeof(out->u.list_vtables.database));
                    advance(&p);
                } else if (accept(&p, QTOK_GROUP) ||
                           (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "group")
                            && (advance(&p), 1))) {
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected group name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, out->u.list_vtables.group,
                             sizeof(out->u.list_vtables.group));
                    advance(&p);
                } else {
                    perr(&p, "expected DATABASE or GROUP after IN");
                    return TSDB_ERR_PARSE;
                }
            }
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* LIST PTABLES [USING <vtable>] [IN DATABASE <db>] [IN GROUP <grp>]
         * Alias: LIST TABLES. */
        if (p.tok.kind == QTOK_IDENT &&
            (ident_ci(&p.tok, "ptables") || ident_ci(&p.tok, "tables"))) {
            out->kind = QAST_STMT_LIST_PTABLES;
            out->u.list_ptables.vtable[0]   = '\0';
            out->u.list_ptables.database[0] = '\0';
            out->u.list_ptables.group[0]    = '\0';
            advance(&p);
            if (accept(&p, QTOK_USING)) {
                if (p.tok.kind != QTOK_IDENT) {
                    perr(&p, "expected vtable name after USING");
                    return TSDB_ERR_PARSE;
                }
                tok_copy(&p.tok, out->u.list_ptables.vtable,
                         sizeof(out->u.list_ptables.vtable));
                advance(&p);
            }
            for (int c = 0; c < 2 && accept(&p, QTOK_IN); c++) {
                if (p.tok.kind == QTOK_IDENT &&
                    (ident_ci(&p.tok, "database") || ident_ci(&p.tok, "db"))) {
                    advance(&p);
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected database name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, out->u.list_ptables.database,
                             sizeof(out->u.list_ptables.database));
                    advance(&p);
                } else if (accept(&p, QTOK_GROUP) ||
                           (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "group")
                            && (advance(&p), 1))) {
                    if (p.tok.kind != QTOK_IDENT) {
                        perr(&p, "expected group name"); return TSDB_ERR_PARSE;
                    }
                    tok_copy(&p.tok, out->u.list_ptables.group,
                             sizeof(out->u.list_ptables.group));
                    advance(&p);
                } else {
                    perr(&p, "expected DATABASE or GROUP after IN");
                    return TSDB_ERR_PARSE;
                }
            }
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
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

        /* LIST FUNCTIONS — "functions" is an IDENT (plural of FUNCTION keyword). */
        if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "functions")) {
            out->kind = QAST_STMT_LIST_FUNCTIONS;
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* LIST MASTERS — Raft master set from the persisted config. */
        if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "masters")) {
            out->kind = QAST_STMT_LIST_MASTERS;
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        /* LIST USERS — "users" is an IDENT (plural of USER keyword). */
        if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "users")) {
            out->kind = QAST_STMT_LIST_USERS;
            advance(&p);
            accept(&p, QTOK_SEMI);
            return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
        }

        perr(&p, "expected GROUPS, DEVICES, or USERS after LIST");
        return TSDB_ERR_PARSE;
    }

    /* ---- JOIN GROUP <name> AS <consumer_id> (TMQ) ---------------------- */
    if (accept(&p, QTOK_JOIN)) {
        if (!accept(&p, QTOK_GROUP)) {
            perr(&p, "expected GROUP after JOIN"); return TSDB_ERR_PARSE;
        }
        out->kind = QAST_STMT_JOIN_GROUP;
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected consumer-group name"); return TSDB_ERR_PARSE;
        }
        tok_copy(&p.tok, out->u.join_group.name,
                 sizeof(out->u.join_group.name));
        advance(&p);
        if (!accept(&p, QTOK_AS)) {
            perr(&p, "expected AS <consumer_id>"); return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected consumer id after AS"); return TSDB_ERR_PARSE;
        }
        tok_copy(&p.tok, out->u.join_group.consumer_id,
                 sizeof(out->u.join_group.consumer_id));
        advance(&p);
        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    /* ---- LEAVE GROUP <name> AS <consumer_id> (TMQ) --------------------- */
    if (accept(&p, QTOK_LEAVE)) {
        if (!accept(&p, QTOK_GROUP)) {
            perr(&p, "expected GROUP after LEAVE"); return TSDB_ERR_PARSE;
        }
        out->kind = QAST_STMT_LEAVE_GROUP;
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected consumer-group name"); return TSDB_ERR_PARSE;
        }
        tok_copy(&p.tok, out->u.leave_group.name,
                 sizeof(out->u.leave_group.name));
        advance(&p);
        if (!accept(&p, QTOK_AS)) {
            perr(&p, "expected AS <consumer_id>"); return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected consumer id after AS"); return TSDB_ERR_PARSE;
        }
        tok_copy(&p.tok, out->u.leave_group.consumer_id,
                 sizeof(out->u.leave_group.consumer_id));
        advance(&p);
        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    /* ---- COMMIT OFFSET <name> [AS <consumer_id>] AT <seq> (TMQ) -------
     * Spec form: `COMMIT OFFSET <name> AT <seq>` applies <seq> to every
     * partition of the group (offsets are per-group, per-partition).
     * Extended form `AS <consumer_id>` scopes the commit to the partitions
     * currently owned by that consumer. consumer_id == "" means group-wide. */
    if (accept(&p, QTOK_COMMIT)) {
        if (!accept(&p, QTOK_OFFSET)) {
            perr(&p, "expected OFFSET after COMMIT"); return TSDB_ERR_PARSE;
        }
        out->kind = QAST_STMT_COMMIT_OFFSET;
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected consumer-group name"); return TSDB_ERR_PARSE;
        }
        tok_copy(&p.tok, out->u.commit_offset.name,
                 sizeof(out->u.commit_offset.name));
        advance(&p);
        out->u.commit_offset.consumer_id[0] = '\0';   /* default: group-wide */
        if (accept(&p, QTOK_AS)) {
            if (p.tok.kind != QTOK_IDENT) {
                perr(&p, "expected consumer id after AS"); return TSDB_ERR_PARSE;
            }
            tok_copy(&p.tok, out->u.commit_offset.consumer_id,
                     sizeof(out->u.commit_offset.consumer_id));
            advance(&p);
        }
        if (!accept(&p, QTOK_AT)) {
            perr(&p, "expected AT <seq>"); return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_NUMBER) {
            perr(&p, "expected integer seq after AT"); return TSDB_ERR_PARSE;
        }
        out->u.commit_offset.seq = p.tok.i;
        advance(&p);
        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    /* ---- EXPORT TABLE <name> TO PARQUET '<dir>' ------------------------- */
    if (accept(&p, QTOK_EXPORT)) {
        if (!accept(&p, QTOK_TABLE)) {
            perr(&p, "expected TABLE after EXPORT"); return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected table name"); return TSDB_ERR_PARSE;
        }
        out->kind = QAST_STMT_EXPORT_PARQUET;
        tok_copy(&p.tok, out->u.export_parquet.table,
                 sizeof(out->u.export_parquet.table));
        advance(&p);
        if (!accept(&p, QTOK_TO)) {
            perr(&p, "expected TO after table name"); return TSDB_ERR_PARSE;
        }
        if (!accept(&p, QTOK_PARQUET)) {
            perr(&p, "expected PARQUET after TO"); return TSDB_ERR_PARSE;
        }
        if (p.tok.kind != QTOK_STRING) {
            perr(&p, "expected '<dir>' path after PARQUET"); return TSDB_ERR_PARSE;
        }
        {
            char *path = str_literal(&p, &p.tok);
            if (!path) return TSDB_ERR_NOMEM;
            size_t n = strlen(path);
            if (n >= sizeof(out->u.export_parquet.out_dir)) {
                n = sizeof(out->u.export_parquet.out_dir) - 1;
            }
            memcpy(out->u.export_parquet.out_dir, path, n);
            out->u.export_parquet.out_dir[n] = '\0';
        }
        advance(&p);
        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    /* ---- GRANT <priv> ON (<table>|*) TO <user> ------------------------- */
    /* ---- REVOKE <priv> ON (<table>|*) FROM <user> ---------------------- */
    if (p.tok.kind == QTOK_GRANT || p.tok.kind == QTOK_REVOKE) {
        int is_grant = (p.tok.kind == QTOK_GRANT);
        advance(&p);

        int priv = 0;
        if (accept(&p, QTOK_SELECT)) {
            priv = TSDB_PRIV_SELECT;
        } else if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "insert")) {
            priv = TSDB_PRIV_INSERT;
            advance(&p);
        } else if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "ddl")) {
            priv = TSDB_PRIV_DDL;
            advance(&p);
        } else if (p.tok.kind == QTOK_IDENT && ident_ci(&p.tok, "all")) {
            priv = TSDB_PRIV_ALL;
            advance(&p);
        } else {
            perr(&p, "expected SELECT, INSERT, DDL, or ALL"); return TSDB_ERR_PARSE;
        }

        if (!accept(&p, QTOK_ON)) {
            perr(&p, "expected ON after privilege"); return TSDB_ERR_PARSE;
        }

        char resource[64];
        if (accept(&p, QTOK_STAR)) {
            snprintf(resource, sizeof(resource), "*");
        } else if (p.tok.kind == QTOK_IDENT) {
            tok_copy(&p.tok, resource, sizeof(resource));
            advance(&p);
        } else {
            perr(&p, "expected table name or *"); return TSDB_ERR_PARSE;
        }

        if (is_grant) {
            if (!accept(&p, QTOK_TO)) {
                perr(&p, "expected TO after resource"); return TSDB_ERR_PARSE;
            }
        } else {
            if (!accept(&p, QTOK_FROM)) {
                perr(&p, "expected FROM after resource"); return TSDB_ERR_PARSE;
            }
        }

        if (p.tok.kind != QTOK_IDENT) {
            perr(&p, "expected user name"); return TSDB_ERR_PARSE;
        }
        if (is_grant) {
            out->kind = QAST_STMT_GRANT;
            tok_copy(&p.tok, out->u.grant.user, sizeof(out->u.grant.user));
            out->u.grant.priv = priv;
            snprintf(out->u.grant.resource, sizeof(out->u.grant.resource),
                     "%s", resource);
        } else {
            out->kind = QAST_STMT_REVOKE;
            tok_copy(&p.tok, out->u.revoke_.user, sizeof(out->u.revoke_.user));
            out->u.revoke_.priv = priv;
            snprintf(out->u.revoke_.resource, sizeof(out->u.revoke_.resource),
                     "%s", resource);
        }
        advance(&p);
        accept(&p, QTOK_SEMI);
        return p.errored ? TSDB_ERR_PARSE : TSDB_OK;
    }

    perr(&p, "expected SELECT, CREATE, DROP, LIST, JOIN, LEAVE, COMMIT, GRANT, REVOKE, or EXPORT");
    return TSDB_ERR_PARSE;
}

/* qparse: legacy SELECT-only wrapper.  Calls qparse_stmt and errors for
 * any non-SELECT statement so callers that only expect SELECT still work. */
/* NOTE: qparse is already defined above this block.  We instead just update
 * tsdb_query in exec.c to call qparse_stmt directly. */
