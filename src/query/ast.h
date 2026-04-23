/* ast.h — QTL abstract syntax tree.
 *
 * QTL (Query Time-series Language) grammar:
 *
 *   query     := SELECT sel_list FROM ident
 *                [ ASOF JOIN ident ON key=key [, key=key]* ]
 *                [ WHERE expr ]
 *                [ SAMPLE BY interval [ FILL (fill) ] ]
 *                [ SESSION(ts_col, gap) | STATE_WINDOW(col) | EVENT_WINDOW(start,end) ]
 *                [ LATEST ON ident [ PARTITION BY ident_list ] ]
 *                [ GROUP BY ident_list ]
 *                [ ORDER BY ident [ ASC | DESC ] ]
 *                [ LIMIT N ]
 *   sel_list  := sel_item ( ',' sel_item )*
 *   sel_item  := '*' | expr [ AS ident ]
 *   expr      := or_expr
 *   or_expr   := and_expr ( OR and_expr )*
 *   and_expr  := cmp_expr ( AND cmp_expr )*
 *   cmp_expr  := add_expr [ ('=' | '!=' | '<' | '<=' | '>' | '>=') add_expr ]
 *   add_expr  := mul_expr ( ('+'|'-') mul_expr )*
 *   mul_expr  := unary_expr ( ('*'|'/'|'%') unary_expr )*
 *   unary_expr:= [ '-' | NOT ] primary
 *   primary   := literal | ident | call | '(' expr ')'
 *   call      := ident '(' [ expr (',' expr)* ] ')'
 *   literal   := NUMBER | STRING | TIMESTAMP
 *   interval  := NUMBER unit            ; unit: ns, us, ms, s, m, h, d
 *   fill      := PREV | NULL | NUMBER
 */
#ifndef TSDB_QUERY_AST_H
#define TSDB_QUERY_AST_H

#include <stddef.h>
#include <stdint.h>
#include "../core/arena.h"
#include "../../include/tsdb.h"
#include "../catalog/group.h"
#include "../catalog/device.h"
#include "../catalog/stable.h"

typedef enum {
    /* Literals */
    QAST_LIT_INT,
    QAST_LIT_FLOAT,
    QAST_LIT_STR,
    QAST_LIT_TS,
    QAST_LIT_NULL,
    /* References */
    QAST_IDENT,
    QAST_STAR,
    /* Operators */
    QAST_NEG,
    QAST_NOT,
    QAST_ADD, QAST_SUB, QAST_MUL, QAST_DIV, QAST_MOD,
    QAST_EQ, QAST_NE, QAST_LT, QAST_LE, QAST_GT, QAST_GE,
    QAST_AND, QAST_OR,
    /* Call */
    QAST_CALL
} qast_kind_t;

typedef struct qast_expr qast_expr_t;

struct qast_expr {
    qast_kind_t kind;
    union {
        int64_t  i;
        double   f;
        char    *s;        /* ident / string / call-name */
        tsdb_ts_t ts;
    } v;
    qast_expr_t *lhs;      /* also used as first operand for unary */
    qast_expr_t *rhs;
    qast_expr_t **args;    /* for QAST_CALL */
    int          nargs;
};

typedef struct {
    qast_expr_t *expr;     /* NULL for '*' */
    char        *alias;    /* may be NULL */
    int          is_star;
} qast_sel_item_t;

typedef enum { QAST_FILL_NONE, QAST_FILL_NULL, QAST_FILL_PREV, QAST_FILL_CONST } qast_fill_kind_t;

typedef struct {
    qast_fill_kind_t kind;
    double           const_v;
} qast_fill_t;

typedef struct {
    int64_t ns;
} qast_interval_t;

typedef enum { QAST_ORDER_ASC, QAST_ORDER_DESC } qast_order_dir_t;

/* Advanced window kind (mutually exclusive with has_sample_by). */
typedef enum {
    QAST_WIN_SAMPLE_BY = 0,  /* existing fixed-interval bucket     */
    QAST_WIN_SESSION   = 1,  /* gap-based: new window when ts gap > threshold */
    QAST_WIN_STATE     = 2,  /* value-change: new window when state col changes */
    QAST_WIN_EVENT     = 3,  /* expression boundary: open/close on bool exprs */
} qast_window_kind_t;

typedef struct {
    qast_sel_item_t *sel;
    int              nsel;

    char            *from;

    /* ASOF JOIN: optional right-side table join.
     * Semantics: for each left row, find the right row with the largest
     * right.ts <= left.ts, among rows where all ON key pairs match.
     * Right columns are NULL when no such row exists. */
    int              has_asof_join;
    char            *asof_table;       /* right table name */
    char           **asof_on_keys_l;   /* left  key column names [asof_n_keys] */
    char           **asof_on_keys_r;   /* right key column names [asof_n_keys] */
    int              asof_n_keys;      /* number of ON key pairs (may be 0) */

    qast_expr_t     *where;

    int              has_sample_by;
    qast_interval_t  sample_by;
    qast_fill_t      fill;

    /* Advanced window (SESSION / STATE_WINDOW / EVENT_WINDOW).
     * Mutually exclusive with has_sample_by. */
    int                has_adv_window;
    qast_window_kind_t adv_window_kind;
    int64_t            session_gap_ns;    /* SESSION: gap threshold in ns          */
    char              *state_col;         /* STATE_WINDOW: column to watch         */
    qast_expr_t       *event_start_expr;  /* EVENT_WINDOW: open-window condition   */
    qast_expr_t       *event_end_expr;    /* EVENT_WINDOW: close-window condition  */

    int              has_latest_on;
    char            *latest_on_col;
    char           **latest_part_cols;
    int              nlatest_part;

    char           **group_by;
    int              ngroup_by;

    /* Top-level PARTITION BY tbname — STable per-child dimension.
     * Valid only when FROM is a super-table; forces row-union result
     * with a synthetic first column 'tbname' (SYMBOL). */
    int              has_partition_by_tbname;

    int              has_order;
    char            *order_col;
    qast_order_dir_t order_dir;

    int              has_limit;
    int64_t          limit;
} qast_query_t;

/* Convenience builders (all allocate from arena). */
qast_expr_t *qast_mk_int(tsdb_arena_t *a, int64_t v);
qast_expr_t *qast_mk_float(tsdb_arena_t *a, double v);
qast_expr_t *qast_mk_str(tsdb_arena_t *a, const char *s);
qast_expr_t *qast_mk_ts(tsdb_arena_t *a, tsdb_ts_t v);
qast_expr_t *qast_mk_ident(tsdb_arena_t *a, const char *s);
qast_expr_t *qast_mk_star(tsdb_arena_t *a);
qast_expr_t *qast_mk_null(tsdb_arena_t *a);
qast_expr_t *qast_mk_unary(tsdb_arena_t *a, qast_kind_t k, qast_expr_t *x);
qast_expr_t *qast_mk_binop(tsdb_arena_t *a, qast_kind_t k, qast_expr_t *l, qast_expr_t *r);
qast_expr_t *qast_mk_call(tsdb_arena_t *a, const char *name, qast_expr_t **args, int nargs);

/* ---- Statement-level AST (DDL + SELECT) --------------------------------- */

typedef enum {
    QAST_STMT_SELECT,          /* existing query */
    /* Database DDL — top of the 3-level hierarchy (DB → Group → Table). */
    QAST_STMT_CREATE_DATABASE,
    QAST_STMT_DROP_DATABASE,
    QAST_STMT_LIST_DATABASES,
    QAST_STMT_CREATE_GROUP,
    QAST_STMT_DROP_GROUP,
    QAST_STMT_LIST_GROUPS,
    QAST_STMT_CREATE_DEVICE,
    QAST_STMT_DROP_DEVICE,
    QAST_STMT_LIST_DEVICES,
    /* VTable = STable (super-table); PTable = child-table.  Aliases exist
     * in the grammar; these two list kinds are new, exposing the 3-level
     * hierarchy through QTL. */
    QAST_STMT_LIST_VTABLES,
    QAST_STMT_LIST_PTABLES,     /* optional arg: vtable filter */
    /* STable DDL */
    QAST_STMT_CREATE_STABLE,
    QAST_STMT_DROP_STABLE,
    QAST_STMT_CREATE_CHILD_TABLE,  /* CREATE TABLE name USING stable TAGS(...) */
    /* Normal table DDL: CREATE TABLE name (col TYPE, ...) TIMESTAMP(ts)
     * [WITH (BLOCK_POINTS=N, PARTITION='hour'|'day')] */
    QAST_STMT_CREATE_TABLE,
    /* ALTER TABLE t ADD COLUMN c TYPE */
    QAST_STMT_ALTER_ADD_COLUMN,
    /* TRUNCATE TABLE t — wipe all rows, keep schema. */
    QAST_STMT_TRUNCATE_TABLE,
    /* DELETE FROM t WHERE <ts_col> <OP> <literal_ts> — partition-level
     * time-range delete.  Only whole partitions fully outside the kept
     * range are removed; boundary partitions are preserved as-is. */
    QAST_STMT_DELETE_RANGE,
    /* TMQ consumer-group statements */
    QAST_STMT_CREATE_CONSUMER_GROUP,  /* CREATE CONSUMER GROUP n ON topic   */
    QAST_STMT_JOIN_GROUP,             /* JOIN GROUP n AS consumer_id        */
    QAST_STMT_LEAVE_GROUP,            /* LEAVE GROUP n AS consumer_id       */
    QAST_STMT_COMMIT_OFFSET,          /* COMMIT OFFSET n AT seq (needs AS)  */
    /* Parquet export */
    QAST_STMT_EXPORT_PARQUET,         /* EXPORT TABLE <name> TO PARQUET '<dir>' */
    /* RBAC statements */
    QAST_STMT_CREATE_USER,            /* CREATE USER n IDENTIFIED BY '...' [ROLE admin|normal] */
    QAST_STMT_DROP_USER,              /* DROP USER n                                          */
    QAST_STMT_LIST_USERS,             /* LIST USERS                                           */
    QAST_STMT_GRANT,                  /* GRANT <priv> ON <table>|* TO <user>                  */
    QAST_STMT_REVOKE,                 /* REVOKE <priv> ON <table>|* FROM <user>               */
    QAST_STMT_ALTER_USER_PASSWORD,    /* ALTER USER n SET PASSWORD '<pw>'                      */
    /* UDF DDL */
    QAST_STMT_CREATE_FUNCTION,        /* CREATE FUNCTION f(T,T) RETURNS T FROM '...' SYMBOL '...' */
    QAST_STMT_DROP_FUNCTION,          /* DROP FUNCTION f                                       */
    QAST_STMT_LIST_FUNCTIONS,         /* LIST FUNCTIONS                                        */
    /* Raft membership (master set).  MVP scope: light wrapper around
     * a persisted config file + propose a CONFIG log entry.  See
     * src/raft/ for the real machinery. */
    QAST_STMT_LIST_MASTERS,           /* LIST MASTERS                                          */
    QAST_STMT_ADD_MASTER,             /* ADD MASTER 'host:port'                                */
    QAST_STMT_REMOVE_MASTER,          /* REMOVE MASTER '<node_id_or_addr>'                     */
} qast_stmt_kind_t;

typedef struct {
    qast_stmt_kind_t kind;
    union {
        qast_query_t query;                              /* QAST_STMT_SELECT */
        /* Database DDL */
        struct { char name[64]; char description[192]; int64_t retention_ns; }
               create_database;                          /* QAST_STMT_CREATE_DATABASE */
        struct { char name[64]; }      drop_database;    /* QAST_STMT_DROP_DATABASE */
        /* LIST_DATABASES has no extra data */
        struct { tsdb_group_t spec; }  create_group;     /* QAST_STMT_CREATE_GROUP */
        struct { char name[64]; }      drop_group;       /* QAST_STMT_DROP_GROUP */
        /* LIST_GROUPS has no extra data */
        struct { tsdb_device_t spec; } create_device;    /* QAST_STMT_CREATE_DEVICE */
        struct { char group[64]; char id[128]; } drop_device;  /* QAST_STMT_DROP_DEVICE */
        struct { char group[64]; }     list_devices;     /* QAST_STMT_LIST_DEVICES; group="" = all */
        /* LIST PTABLES [USING <vtable>] [IN DATABASE <db>] [IN GROUP <grp>]
         * Empty string for each filter means "no filter for that axis". */
        struct { char vtable[64]; char database[64]; char group[64]; }
            list_ptables;
        /* LIST VTABLES [IN DATABASE <db>] [IN GROUP <grp>] — same filter
         * semantics as list_ptables. */
        struct { char database[64]; char group[64]; }
            list_vtables;
        /* Raft membership */
        struct { char target[96]; }    add_master;      /* "host:port" */
        struct { char target[96]; }    remove_master;   /* node_id or addr */
        /* STable DDL */
        struct { tsdb_stable_t spec; }       create_stable;       /* QAST_STMT_CREATE_STABLE */
        struct { char name[64]; }            drop_stable;          /* QAST_STMT_DROP_STABLE */
        struct { tsdb_child_table_t spec; }  create_child_table;   /* QAST_STMT_CREATE_CHILD_TABLE */
        /* QAST_STMT_CREATE_TABLE — normal table with explicit column list. */
        struct {
            char                     name[64];
            int                      ncols;
            char                     col_names[TSDB_STABLE_MAX_COLS][64];
            tsdb_type_t              col_types[TSDB_STABLE_MAX_COLS];
            char                     ts_col[64];
            int                      block_points;    /* 0 = db default   */
            int                      partition_hour;  /* 0 = DAY, 1 = HOUR */
        } create_table;
        /* ALTER TABLE t ADD COLUMN c TYPE */
        struct {
            char         table[64];
            char         col_name[64];
            tsdb_type_t  col_type;
        } alter_add_column;
        /* TRUNCATE TABLE t */
        struct { char name[64]; } truncate_table;
        /* DELETE FROM t WHERE ts <op> cutoff
         *   op_lt:  1 for '<' / '<=', 0 for '>' / '>=' (keep-side semantics).
         *   inclusive: 1 if '<=' or '>='. */
        struct {
            char     table[64];
            int64_t  cutoff_ns;   /* nanosecond timestamp literal */
            int8_t   op_lt;       /* 1 = delete ts <(=) cutoff, 0 = ts >(=) cutoff */
            int8_t   inclusive;   /* 1 = include boundary row (<=, >=) */
        } delete_range;
        /* TMQ statements (all use short fixed-size names) */
        struct { char name[64]; char topic[64]; } create_consumer_group;
        struct { char name[64]; char consumer_id[64]; } join_group;
        struct { char name[64]; char consumer_id[64]; } leave_group;
        struct { char name[64]; char consumer_id[64]; int64_t seq; } commit_offset;
        /* Parquet export: EXPORT TABLE <table> TO PARQUET '<dir>' */
        struct { char table[64]; char out_dir[1024]; } export_parquet;
        /* RBAC */
        struct {
            char  name[64];
            char  password[128];
            int   role;   /* 0 = NORMAL, 1 = ADMIN */
        } create_user;
        struct { char name[64]; } drop_user;
        struct {
            char  user[64];
            int   priv;           /* bitmask: TSDB_PRIV_{SELECT,INSERT,DDL,ALL} */
            char  resource[64];   /* table name or "*" */
        } grant, revoke_;
        struct {
            char name[64];
            char password[128];
        } alter_user_password;
        /* UDF DDL.  arg_types[] uses tsdb_type_t from include/tsdb.h
         * (which matches tsdb_udf_type_t 1-for-1 in v1). */
        struct {
            char         name[64];
            char         so_path[1024];
            char         symbol[128];
            int          nargs;
            tsdb_type_t  arg_types[8];   /* TSDB_UDF_MAX_ARGS in tsdb_udf.h */
            tsdb_type_t  ret_type;
        } create_function;
        struct { char name[64]; } drop_function;
    } u;
} qast_stmt_t;

#endif
