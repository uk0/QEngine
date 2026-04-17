#include "ast.h"
#include <string.h>

static qast_expr_t *mk(tsdb_arena_t *a, qast_kind_t k) {
    qast_expr_t *e = tsdb_arena_alloc(a, sizeof(*e));
    if (!e) return NULL;
    memset(e, 0, sizeof(*e));
    e->kind = k;
    return e;
}

qast_expr_t *qast_mk_int(tsdb_arena_t *a, int64_t v) {
    qast_expr_t *e = mk(a, QAST_LIT_INT);
    if (e) e->v.i = v;
    return e;
}

qast_expr_t *qast_mk_float(tsdb_arena_t *a, double v) {
    qast_expr_t *e = mk(a, QAST_LIT_FLOAT);
    if (e) e->v.f = v;
    return e;
}

qast_expr_t *qast_mk_str(tsdb_arena_t *a, const char *s) {
    qast_expr_t *e = mk(a, QAST_LIT_STR);
    if (e) e->v.s = tsdb_arena_strdup(a, s);
    return e;
}

qast_expr_t *qast_mk_ts(tsdb_arena_t *a, tsdb_ts_t v) {
    qast_expr_t *e = mk(a, QAST_LIT_TS);
    if (e) e->v.ts = v;
    return e;
}

qast_expr_t *qast_mk_ident(tsdb_arena_t *a, const char *s) {
    qast_expr_t *e = mk(a, QAST_IDENT);
    if (e) e->v.s = tsdb_arena_strdup(a, s);
    return e;
}

qast_expr_t *qast_mk_star(tsdb_arena_t *a) { return mk(a, QAST_STAR); }
qast_expr_t *qast_mk_null(tsdb_arena_t *a) { return mk(a, QAST_LIT_NULL); }

qast_expr_t *qast_mk_unary(tsdb_arena_t *a, qast_kind_t k, qast_expr_t *x) {
    qast_expr_t *e = mk(a, k);
    if (e) e->lhs = x;
    return e;
}

qast_expr_t *qast_mk_binop(tsdb_arena_t *a, qast_kind_t k, qast_expr_t *l, qast_expr_t *r) {
    qast_expr_t *e = mk(a, k);
    if (e) { e->lhs = l; e->rhs = r; }
    return e;
}

qast_expr_t *qast_mk_call(tsdb_arena_t *a, const char *name, qast_expr_t **args, int nargs) {
    qast_expr_t *e = mk(a, QAST_CALL);
    if (!e) return NULL;
    e->v.s = tsdb_arena_strdup(a, name);
    e->nargs = nargs;
    if (nargs > 0) {
        e->args = tsdb_arena_alloc(a, sizeof(qast_expr_t *) * (size_t)nargs);
        if (!e->args) return NULL;
        memcpy(e->args, args, sizeof(qast_expr_t *) * (size_t)nargs);
    }
    return e;
}
