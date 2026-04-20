#include "lex.h"
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static int eq_ci(const char *a, size_t an, const char *b) {
    size_t bn = strlen(b);
    if (an != bn) return 0;
    for (size_t i = 0; i < an; i++) {
        if (tolower((unsigned char)a[i]) != tolower((unsigned char)b[i])) return 0;
    }
    return 1;
}

static struct { const char *kw; qtok_kind_t k; } KEYWORDS[] = {
    {"select",    QTOK_SELECT},   {"from",      QTOK_FROM},      {"where",     QTOK_WHERE},
    {"as",        QTOK_AS},       {"and",       QTOK_AND},       {"or",        QTOK_OR},
    {"not",       QTOK_NOT},      {"sample",    QTOK_SAMPLE},    {"by",        QTOK_BY},
    {"fill",      QTOK_FILL},     {"prev",      QTOK_PREV},      {"null",      QTOK_NULL_KW},
    {"latest",    QTOK_LATEST},   {"on",        QTOK_ON},        {"partition", QTOK_PARTITION},
    {"group",     QTOK_GROUP},    {"order",     QTOK_ORDER},     {"asc",       QTOK_ASC},
    {"desc",      QTOK_DESC},     {"limit",     QTOK_LIMIT},     {"in",        QTOK_IN},
    {"between",   QTOK_BETWEEN},  {"true",      QTOK_TRUE},      {"false",     QTOK_FALSE},
    /* DDL keywords */
    {"create",    QTOK_CREATE},   {"drop",      QTOK_DROP},      {"list",      QTOK_LIST},
    {"truncate",  QTOK_TRUNCATE}, {"delete",    QTOK_DELETE},
    {"device",    QTOK_DEVICE},
    {"retention", QTOK_RETENTION},{"profile",   QTOK_PROFILE},   {"region",    QTOK_REGION},
    {"replica",   QTOK_REPLICA},  {"factor",    QTOK_FACTOR},
    /* STable DDL */
    {"stable",    QTOK_STABLE},   {"using",     QTOK_USING},     {"tags",      QTOK_TAGS},
    {"table",     QTOK_TABLE},
    /* ALTER TABLE ADD COLUMN */
    {"alter",     QTOK_ALTER},    {"add",       QTOK_ADD},       {"column",    QTOK_COLUMN},
    /* ASOF JOIN */
    {"asof",         QTOK_ASOF},         {"join",         QTOK_JOIN},
    /* Advanced windows — state_window / event_window lex as single IDENT
     * because '_' is an ident character; map them here directly. */
    {"session",      QTOK_SESSION},
    {"state_window", QTOK_STATE_WINDOW},
    {"event_window", QTOK_EVENT_WINDOW},
    /* TMQ consumer-group keywords */
    {"consumer",     QTOK_CONSUMER},
    {"commit",       QTOK_COMMIT},
    {"offset",       QTOK_OFFSET},
    {"at",           QTOK_AT},
    {"leave",        QTOK_LEAVE},
    {"topic",        QTOK_TOPIC},
    /* Parquet export */
    {"export",       QTOK_EXPORT},
    {"to",           QTOK_TO},
    {"parquet",      QTOK_PARQUET},
    /* UDF DDL */
    {"function",     QTOK_FUNCTION},
    {"returns",      QTOK_RETURNS},
    /* RBAC keywords */
    {"user",         QTOK_USER},
    {"identified",   QTOK_IDENTIFIED},
    {"role",         QTOK_ROLE},
    {"admin",        QTOK_ADMIN},
    {"normal",       QTOK_NORMAL},
    {"grant",        QTOK_GRANT},
    {"revoke",       QTOK_REVOKE},
    {"password",     QTOK_PASSWORD},
};

static qtok_kind_t keyword_lookup(const char *s, size_t n) {
    for (size_t i = 0; i < sizeof(KEYWORDS) / sizeof(KEYWORDS[0]); i++) {
        if (eq_ci(s, n, KEYWORDS[i].kw)) return KEYWORDS[i].k;
    }
    return QTOK_IDENT;
}

const char *qtok_name(qtok_kind_t k) {
    switch (k) {
    case QTOK_EOF: return "EOF";
    case QTOK_LPAREN: return "("; case QTOK_RPAREN: return ")";
    case QTOK_COMMA: return ","; case QTOK_SEMI: return ";";
    case QTOK_DOT: return "."; case QTOK_STAR: return "*";
    case QTOK_PLUS: return "+"; case QTOK_MINUS: return "-";
    case QTOK_SLASH: return "/"; case QTOK_PERCENT: return "%";
    case QTOK_EQ: return "="; case QTOK_NE: return "!=";
    case QTOK_LT: return "<"; case QTOK_LE: return "<=";
    case QTOK_GT: return ">"; case QTOK_GE: return ">=";
    case QTOK_IDENT: return "ident"; case QTOK_NUMBER: return "number";
    case QTOK_FLOAT: return "float"; case QTOK_STRING: return "string";
    case QTOK_INTERVAL: return "interval";
    case QTOK_SELECT: return "SELECT"; case QTOK_FROM: return "FROM";
    case QTOK_WHERE: return "WHERE"; case QTOK_AS: return "AS";
    case QTOK_AND: return "AND"; case QTOK_OR: return "OR"; case QTOK_NOT: return "NOT";
    case QTOK_SAMPLE: return "SAMPLE"; case QTOK_BY: return "BY";
    case QTOK_FILL: return "FILL"; case QTOK_PREV: return "PREV"; case QTOK_NULL_KW: return "NULL";
    case QTOK_LATEST: return "LATEST"; case QTOK_ON: return "ON"; case QTOK_PARTITION: return "PARTITION";
    case QTOK_GROUP: return "GROUP"; case QTOK_ORDER: return "ORDER";
    case QTOK_ASC: return "ASC"; case QTOK_DESC: return "DESC"; case QTOK_LIMIT: return "LIMIT";
    case QTOK_IN: return "IN"; case QTOK_BETWEEN: return "BETWEEN";
    case QTOK_TRUE: return "true"; case QTOK_FALSE: return "false";
    case QTOK_CREATE: return "CREATE"; case QTOK_DROP: return "DROP";
    case QTOK_LIST: return "LIST"; case QTOK_DEVICE: return "DEVICE";
    case QTOK_TRUNCATE: return "TRUNCATE"; case QTOK_DELETE: return "DELETE";
    case QTOK_RETENTION: return "RETENTION"; case QTOK_PROFILE: return "PROFILE";
    case QTOK_REGION: return "REGION"; case QTOK_REPLICA: return "REPLICA";
    case QTOK_FACTOR: return "FACTOR";
    case QTOK_STABLE: return "STABLE";
    case QTOK_USING: return "USING";
    case QTOK_TAGS: return "TAGS";
    case QTOK_TABLE: return "TABLE";
    case QTOK_ALTER: return "ALTER";
    case QTOK_ADD:   return "ADD";
    case QTOK_COLUMN: return "COLUMN";
    case QTOK_ASOF: return "ASOF";
    case QTOK_JOIN: return "JOIN";
    case QTOK_SESSION:      return "SESSION";
    case QTOK_STATE_WINDOW: return "STATE_WINDOW";
    case QTOK_EVENT_WINDOW: return "EVENT_WINDOW";
    case QTOK_CONSUMER:     return "CONSUMER";
    case QTOK_COMMIT:       return "COMMIT";
    case QTOK_OFFSET:       return "OFFSET";
    case QTOK_AT:           return "AT";
    case QTOK_LEAVE:        return "LEAVE";
    case QTOK_TOPIC:        return "TOPIC";
    case QTOK_EXPORT:       return "EXPORT";
    case QTOK_TO:           return "TO";
    case QTOK_PARQUET:      return "PARQUET";
    case QTOK_FUNCTION:     return "FUNCTION";
    case QTOK_RETURNS:      return "RETURNS";
    case QTOK_USER:         return "USER";
    case QTOK_IDENTIFIED:   return "IDENTIFIED";
    case QTOK_ROLE:         return "ROLE";
    case QTOK_ADMIN:        return "ADMIN";
    case QTOK_NORMAL:       return "NORMAL";
    case QTOK_GRANT:        return "GRANT";
    case QTOK_REVOKE:       return "REVOKE";
    case QTOK_PASSWORD:     return "PASSWORD";
    case QTOK_ERR: return "<error>";
    }
    return "?";
}

void qlex_init(qlex_t *l, const char *src) {
    l->src = src;
    l->cur = src;
    l->line = 1;
    l->col = 1;
    l->err[0] = 0;
}

static void skip_ws(qlex_t *l) {
    while (*l->cur) {
        unsigned char c = (unsigned char)*l->cur;
        if (c == ' ' || c == '\t' || c == '\r') { l->cur++; l->col++; }
        else if (c == '\n') { l->cur++; l->line++; l->col = 1; }
        else if (c == '-' && l->cur[1] == '-') {          /* line comment */
            while (*l->cur && *l->cur != '\n') l->cur++;
        } else break;
    }
}

static int is_ident_start(int c) { return isalpha(c) || c == '_'; }
static int is_ident_rest(int c)  { return isalnum(c) || c == '_'; }

static int is_interval_unit(const char *s, size_t n) {
    if (n == 2 && (s[0]=='n'||s[0]=='N') && (s[1]=='s'||s[1]=='S')) return 1;
    if (n == 2 && (s[0]=='u'||s[0]=='U') && (s[1]=='s'||s[1]=='S')) return 1;
    if (n == 2 && (s[0]=='m'||s[0]=='M') && (s[1]=='s'||s[1]=='S')) return 1;
    if (n == 1 && (s[0]=='s'||s[0]=='S'||s[0]=='m'||s[0]=='M'||s[0]=='h'||s[0]=='H'||s[0]=='d'||s[0]=='D')) return 1;
    return 0;
}

int qlex_next(qlex_t *l, qtok_t *out) {
    skip_ws(l);
    memset(out, 0, sizeof(*out));
    out->start = l->cur;
    out->line = l->line;
    out->col = l->col;

    char c = *l->cur;
    if (c == 0) { out->kind = QTOK_EOF; return TSDB_OK; }

    /* Punctuation and operators */
    switch (c) {
    case '(': out->kind = QTOK_LPAREN; l->cur++; out->len = 1; return TSDB_OK;
    case ')': out->kind = QTOK_RPAREN; l->cur++; out->len = 1; return TSDB_OK;
    case ',': out->kind = QTOK_COMMA;  l->cur++; out->len = 1; return TSDB_OK;
    case ';': out->kind = QTOK_SEMI;   l->cur++; out->len = 1; return TSDB_OK;
    case '*': out->kind = QTOK_STAR;   l->cur++; out->len = 1; return TSDB_OK;
    case '+': out->kind = QTOK_PLUS;   l->cur++; out->len = 1; return TSDB_OK;
    case '/': out->kind = QTOK_SLASH;  l->cur++; out->len = 1; return TSDB_OK;
    case '%': out->kind = QTOK_PERCENT;l->cur++; out->len = 1; return TSDB_OK;
    case '=': out->kind = QTOK_EQ;     l->cur++; out->len = 1; return TSDB_OK;
    case '-':
        /* Could be minus or start of negative number; leave disambiguation to parser */
        out->kind = QTOK_MINUS; l->cur++; out->len = 1; return TSDB_OK;
    case '!':
        if (l->cur[1] == '=') { out->kind = QTOK_NE; l->cur += 2; out->len = 2; return TSDB_OK; }
        snprintf(l->err, sizeof(l->err), "unexpected '!'"); out->kind = QTOK_ERR; return TSDB_ERR_PARSE;
    case '<':
        if (l->cur[1] == '=') { out->kind = QTOK_LE; l->cur += 2; out->len = 2; return TSDB_OK; }
        if (l->cur[1] == '>') { out->kind = QTOK_NE; l->cur += 2; out->len = 2; return TSDB_OK; }
        out->kind = QTOK_LT; l->cur++; out->len = 1; return TSDB_OK;
    case '>':
        if (l->cur[1] == '=') { out->kind = QTOK_GE; l->cur += 2; out->len = 2; return TSDB_OK; }
        out->kind = QTOK_GT; l->cur++; out->len = 1; return TSDB_OK;
    case '.':
        /* Could be decimal if followed by digit: .5 */
        if (isdigit((unsigned char)l->cur[1])) break;  /* fall through to number handler */
        out->kind = QTOK_DOT; l->cur++; out->len = 1; return TSDB_OK;
    }

    /* String literals */
    if (c == '\'') {
        const char *start = ++l->cur;
        while (*l->cur) {
            if (*l->cur == '\'' && l->cur[1] == '\'') { l->cur += 2; continue; }
            if (*l->cur == '\'') break;
            l->cur++;
        }
        if (*l->cur != '\'') {
            snprintf(l->err, sizeof(l->err), "unterminated string"); out->kind = QTOK_ERR; return TSDB_ERR_PARSE;
        }
        out->start = start;
        out->len = (size_t)(l->cur - start);
        out->kind = QTOK_STRING;
        l->cur++; /* closing quote */
        return TSDB_OK;
    }

    /* Numbers */
    if (isdigit((unsigned char)c) || (c == '.' && isdigit((unsigned char)l->cur[1]))) {
        const char *s = l->cur;
        int is_float = (c == '.');
        while (isdigit((unsigned char)*l->cur)) l->cur++;
        if (*l->cur == '.' && !is_float) {
            is_float = 1; l->cur++;
            while (isdigit((unsigned char)*l->cur)) l->cur++;
        }
        if (*l->cur == 'e' || *l->cur == 'E') {
            is_float = 1; l->cur++;
            if (*l->cur == '+' || *l->cur == '-') l->cur++;
            while (isdigit((unsigned char)*l->cur)) l->cur++;
        }
        /* Interval suffix? */
        if (!is_float && is_ident_start((unsigned char)*l->cur)) {
            const char *u = l->cur;
            while (is_ident_rest((unsigned char)*l->cur)) l->cur++;
            size_t ulen = (size_t)(l->cur - u);
            if (is_interval_unit(u, ulen)) {
                out->kind = QTOK_INTERVAL;
                out->start = s;
                out->len = (size_t)(l->cur - s);
                out->i = qlex_parse_interval(s, out->len);
                if (out->i < 0) { snprintf(l->err, sizeof(l->err), "bad interval"); out->kind = QTOK_ERR; return TSDB_ERR_PARSE; }
                return TSDB_OK;
            }
            /* Not an interval: rewind the unit part */
            l->cur = u;
        }
        out->start = s;
        out->len = (size_t)(l->cur - s);
        if (is_float) {
            out->kind = QTOK_FLOAT;
            out->f = strtod(s, NULL);
        } else {
            out->kind = QTOK_NUMBER;
            out->i = strtoll(s, NULL, 10);
        }
        return TSDB_OK;
    }

    /* Identifiers / keywords */
    if (is_ident_start((unsigned char)c)) {
        const char *s = l->cur;
        while (is_ident_rest((unsigned char)*l->cur)) l->cur++;
        size_t n = (size_t)(l->cur - s);
        out->start = s;
        out->len = n;
        out->kind = keyword_lookup(s, n);
        return TSDB_OK;
    }

    snprintf(l->err, sizeof(l->err), "unexpected char '%c'", c);
    out->kind = QTOK_ERR;
    return TSDB_ERR_PARSE;
}

/* Parse a sequence like "5m" "100ms" "2h30m" — sum of (num, unit) pairs. */
int64_t qlex_parse_interval(const char *s, size_t n) {
    int64_t total = 0;
    size_t i = 0;
    while (i < n) {
        /* number */
        int64_t v = 0;
        size_t start = i;
        while (i < n && isdigit((unsigned char)s[i])) { v = v * 10 + (s[i] - '0'); i++; }
        if (i == start) return -1;

        /* unit */
        size_t us = i;
        while (i < n && isalpha((unsigned char)s[i])) i++;
        size_t ulen = i - us;
        const char *u = s + us;
        int64_t mul = -1;
        if (ulen == 2 && (u[0]=='n'||u[0]=='N') && (u[1]=='s'||u[1]=='S')) mul = 1;
        else if (ulen == 2 && (u[0]=='u'||u[0]=='U') && (u[1]=='s'||u[1]=='S')) mul = 1000LL;
        else if (ulen == 2 && (u[0]=='m'||u[0]=='M') && (u[1]=='s'||u[1]=='S')) mul = 1000000LL;
        else if (ulen == 1) {
            switch (*u) {
            case 's': case 'S': mul = 1000000000LL; break;
            case 'm': case 'M': mul = 60LL * 1000000000LL; break;
            case 'h': case 'H': mul = 3600LL * 1000000000LL; break;
            case 'd': case 'D': mul = 86400LL * 1000000000LL; break;
            }
        }
        if (mul < 0) return -1;
        total += v * mul;
    }
    return total;
}
