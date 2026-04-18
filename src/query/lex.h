/* lex.h — QTL lexer.
 *
 * Simple hand-written tokenizer over a zero-terminated string. Keywords are
 * matched case-insensitively. String literals use single quotes with '' as
 * escape.
 */
#ifndef TSDB_QUERY_LEX_H
#define TSDB_QUERY_LEX_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

typedef enum {
    QTOK_EOF = 0,

    /* Punctuation */
    QTOK_LPAREN, QTOK_RPAREN,
    QTOK_COMMA, QTOK_SEMI, QTOK_DOT, QTOK_STAR,

    /* Operators */
    QTOK_PLUS, QTOK_MINUS, QTOK_SLASH, QTOK_PERCENT,
    QTOK_EQ, QTOK_NE, QTOK_LT, QTOK_LE, QTOK_GT, QTOK_GE,

    /* Literals */
    QTOK_IDENT, QTOK_NUMBER, QTOK_FLOAT, QTOK_STRING, QTOK_INTERVAL,

    /* Keywords */
    QTOK_SELECT, QTOK_FROM, QTOK_WHERE, QTOK_AS,
    QTOK_AND, QTOK_OR, QTOK_NOT,
    QTOK_SAMPLE, QTOK_BY, QTOK_FILL, QTOK_PREV, QTOK_NULL_KW,
    QTOK_LATEST, QTOK_ON, QTOK_PARTITION,
    QTOK_GROUP, QTOK_ORDER, QTOK_ASC, QTOK_DESC, QTOK_LIMIT,
    QTOK_IN, QTOK_BETWEEN,
    QTOK_TRUE, QTOK_FALSE,

    /* DDL keywords (IoT Group/Device) */
    QTOK_CREATE, QTOK_DROP, QTOK_LIST,
    QTOK_DEVICE,
    QTOK_RETENTION, QTOK_PROFILE, QTOK_REGION,
    QTOK_REPLICA, QTOK_FACTOR,

    /* STable DDL keywords */
    QTOK_STABLE, QTOK_USING, QTOK_TAGS,
    QTOK_TABLE,

    /* ALTER TABLE ADD COLUMN */
    QTOK_ALTER, QTOK_ADD, QTOK_COLUMN,

    /* ASOF JOIN keywords */
    QTOK_ASOF, QTOK_JOIN,

    /* Advanced window keywords */
    QTOK_SESSION,         /* SESSION(ts_col, gap_interval)          */
    QTOK_STATE_WINDOW,    /* STATE_WINDOW(state_col)                 */
    QTOK_EVENT_WINDOW,    /* EVENT_WINDOW(start_expr, end_expr)      */

    /* TMQ consumer-group keywords */
    QTOK_CONSUMER,        /* CONSUMER GROUP <name> ON <topic>       */
    QTOK_COMMIT,          /* COMMIT OFFSET                           */
    QTOK_OFFSET,
    QTOK_AT,
    QTOK_LEAVE,           /* LEAVE GROUP                             */
    QTOK_TOPIC,           /* reserved (not required by current grammar) */

    /* Parquet export */
    QTOK_EXPORT,          /* EXPORT TABLE <name> TO PARQUET '<dir>'   */
    QTOK_TO,
    QTOK_PARQUET,

    /* RBAC keywords — see CREATE USER / GRANT / REVOKE / ALTER USER. */
    QTOK_USER,            /* CREATE USER / DROP USER / ALTER USER     */
    QTOK_IDENTIFIED,      /* IDENTIFIED BY '<password>'               */
    QTOK_ROLE,            /* ROLE ADMIN | NORMAL                      */
    QTOK_ADMIN,
    QTOK_NORMAL,
    QTOK_GRANT,           /* GRANT <priv> ON <resource> TO <user>     */
    QTOK_REVOKE,
    QTOK_PASSWORD,        /* ALTER USER ... SET PASSWORD '<pw>'       */

    QTOK_ERR
} qtok_kind_t;

typedef struct {
    qtok_kind_t  kind;
    const char  *start;     /* points into original source */
    size_t       len;
    int64_t      i;
    double       f;
    int          line;
    int          col;
} qtok_t;

typedef struct {
    const char *src;
    const char *cur;
    int         line;
    int         col;
    char        err[128];
} qlex_t;

void qlex_init(qlex_t *l, const char *src);
int  qlex_next(qlex_t *l, qtok_t *out);   /* returns TSDB_OK or TSDB_ERR_PARSE */

const char *qtok_name(qtok_kind_t k);

/* Parse an interval literal: "5m", "100ms", "2h30m" (simple sum).
 * Returns nanoseconds, or -1 on error. */
int64_t qlex_parse_interval(const char *s, size_t n);

#endif
