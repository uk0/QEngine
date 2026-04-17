#include "parse.h"
#include "lex.h"
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>

typedef struct {
    qlex_t       lex;
    qtok_t       tok;
    tsdb_arena_t *arena;
    char        *err;
    size_t       errcap;
    int          errored;
} parser_t;

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

    if (accept(&p, QTOK_WHERE)) {
        q->where = parse_expr(&p);
        if (!q->where) return TSDB_ERR_PARSE;
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
