/*
 * MySQL port driver.
 *
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include "io.h"
#include "log.h"

#include <ei.h>
#include <erl_interface.h>
#include <errno.h>
#include <mysql.h>
#include <stdlib.h>
#include <string.h>

typedef u_int32_t msglen_t;

const char *CONNECT_MSG      = "sql_connect";
const char *QUERY_MSG        = "sql_query";
const char *PARAM_QUERY_MSG  = "sql_param_query";
const char *SELECT_COUNT_MSG = "sql_select_count";
const char *SELECT_MSG       = "sql_select";
const char *FIRST_MSG        = "sql_first";
const char *LAST_MSG         = "sql_last";
const char *NEXT_MSG         = "sql_next";
const char *PREV_MSG         = "sql_prev";

const char *NULL_SQL      = "null";
const char *NUMERIC_SQL   = "sql_numeric";
const char *DECIMAL_SQL   = "sql_decimal";
const char *FLOAT_SQL     = "sql_float";
const char *CHAR_SQL      = "sql_char";
const char *VARCHAR_SQL   = "sql_varchar";
const char *TIMESTAMP_SQL = "sql_timestamp";
const char *INTEGER_SQL   = "sql_integer";

const char *SELECT_ABSOLUTE = "absolute";
const char *SELECT_RELATIVE = "relative";
const char *SELECT_NEXT     = "next";

my_bool TRUTHY = 1;
my_bool FALSY  = 0;

MYSQL dbh;
MYSQL_BIND *r_bind = NULL;
MYSQL_FIELD *fields = NULL;
MYSQL_STMT *sth = NULL;
MYSQL_RES *results = NULL;
my_ulonglong numrows = 0;
my_ulonglong resultoffset = 0; // The index of the next row to read.
unsigned int numfields = 0;

void *safe_malloc(size_t size);
ETERM *read_msg();
int write_msg(ETERM *msg);

void *
safe_malloc(size_t size)
{
  void *rc;

  rc = malloc(size);
  if (rc == NULL) {
    ETERM *resp;

    resp = erl_format("{error, {malloc, ~i}}", size);
    write_msg(resp);
    erl_free_term(resp);

    exit(3);
  }

  return rc;
}

ETERM *
read_msg()
{
  ETERM *msg;
  unsigned char *buf;
  msglen_t len;

  if (restartable_read((unsigned char *)&len, sizeof(len)) == -1) {
    if (errno == 0) {
      logmsg("INFO: got end of file from Erlang process, shutting down.");
      exit(0);
    }

    logmsg("ERROR: couldn't read %d byte message prefix: %s.",
           sizeof(len), strerror(errno));

    exit(2);
  }

  len = ntohl(len);
  buf = (unsigned char *)safe_malloc(len);
  if (restartable_read(buf, len) == -1) {
    logmsg("ERROR: couldn't read %d byte message: %s.",
           len, strerror(errno));

    free(buf);
    exit(2);
  }

  msg = erl_decode(buf);
  free(buf);

  return msg;
}

int
write_msg(ETERM *msg)
{
  unsigned char *buf;
  msglen_t nlen, buflen;

  buflen = erl_term_len(msg);
  buf = (unsigned char *)safe_malloc(buflen);
  erl_encode(msg, buf);
  erl_free_term(msg);

  nlen = htonl(buflen);
  if (restartable_write((unsigned char *)&nlen, sizeof(nlen)) == -1) {
    free(buf);
    return -1;
  }
  if (restartable_write(buf, buflen) == -1) {
    free(buf);
    return -1;
  }
  free(buf);

  return 0;
}

void
set_mysql_results(MYSQL_STMT *handle)
{
  int i;

  /* Clear any old statement handles. */
  if (sth) {
    numrows = 0;
    mysql_stmt_close(sth);
  }
  sth = handle;

  /* Get result metadata. */
  if (results) {
    mysql_free_result(results);
  }
  results = mysql_stmt_result_metadata(sth);

  /* Buffer results. */
  if (r_bind) {
    for (i = 0; i < numfields; i++) {
      free(r_bind[i].buffer);
      free(r_bind[i].is_null);
      free(r_bind[i].length);
      free(r_bind[i].error);
    }
    free(r_bind);
  }

  numfields = mysql_num_fields(results);
  fields = mysql_fetch_fields(results);

  r_bind = safe_malloc(numfields * sizeof(MYSQL_BIND));
  memset(r_bind, 0, numfields * sizeof(MYSQL_BIND));
  for (i = 0; i < numfields; i++) {
    r_bind[i].buffer_type   = fields[i].type;
    r_bind[i].buffer_length = fields[i].length;
    r_bind[i].buffer        = safe_malloc(fields[i].length);
    r_bind[i].is_null       = safe_malloc(sizeof(*r_bind[i].is_null));
    r_bind[i].length        = safe_malloc(sizeof(*r_bind[i].length));
    r_bind[i].error         = safe_malloc(sizeof(*r_bind[i].error));
  }
  mysql_stmt_bind_result(sth, r_bind);

  mysql_stmt_store_result(sth);

  resultoffset = 0;
  numrows = mysql_stmt_num_rows(sth);
}

ETERM *
make_cols()
{
  ETERM **cols, *rc;
  unsigned int i;

  cols = (ETERM **)safe_malloc(numfields * sizeof(ETERM *));
  for (i = 0; i < numfields; i++)
    cols[i] = erl_mk_string(fields[i].name);

  rc = erl_mk_list(cols, numfields);

  for (i = 0; i < numfields; i++)
    erl_free_term(cols[i]);
  free(cols);

  return rc;
}

ETERM *
make_row()
{
  ETERM **rowtup, *rc;
  unsigned int i;

  rowtup = (ETERM **)safe_malloc(numfields * sizeof(ETERM *));
  for (i = 0; i < numfields; i++) {
    if (*r_bind[i].is_null)
      rowtup[i] = erl_mk_atom("null");
    else
      rowtup[i] = erl_mk_estring(r_bind[i].buffer, *r_bind[i].length);
  }

  rc = erl_mk_tuple(rowtup, numfields);
  if (rc == NULL) {
    ETERM *resp;

    resp = erl_format("{error, {erl_mk_tuple, ~i}}", numfields);
    write_msg(resp);
    erl_free_term(resp);
    exit(3);
  }

  for (i = 0; i < numfields; i++)
    erl_free_term(rowtup[i]);
  free(rowtup);

  return rc;
}

ETERM *
make_rows(my_ulonglong count)
{
  ETERM **rows, *rc;
  unsigned int i;

  rows = (ETERM **)safe_malloc(numrows * sizeof(ETERM *));
  for (i = 0; i < count; i++) {
    ETERM *rt;

    switch (mysql_stmt_fetch(sth)) {
    case 0:
      rt = make_row();
      rows[i] = erl_format("~w", rt);
      erl_free_term(rt);
      break;
    case MYSQL_NO_DATA:
      rt = erl_format("{error, {mysql_error, no_data}}");
      write_msg(rt);
      erl_free_term(rt);
      exit(3);
    case MYSQL_DATA_TRUNCATED:
      rt = erl_format("{error, {mysql_error, data_truncated}}");
      write_msg(rt);
      erl_free_term(rt);
      exit(3);
    default:
      rt = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_stmt_errno(sth), mysql_stmt_error(sth));
      write_msg(rt);
      erl_free_term(rt);
      exit(3);
    }
  }

  rc = erl_mk_list(rows, count);

  for (i = 0; i < count; i++)
    erl_free_term(rows[i]);
  free(rows);

  return rc;
}

ETERM *
handle_mysql_result()
{
  ETERM *ecols, *erows, *resp;

  ecols = make_cols();
  erows = make_rows(numrows);
  resultoffset = numrows;

  resp = erl_format("{selected, ~w, ~w}", ecols, erows);

  erl_free_term(ecols);
  erl_free_term(erows);

  return resp;
}

void
handle_connect(ETERM *msg)
{
  ETERM *resp, *tmp;
  char *host, *db_name, *user, *passwd;
  int port;

  tmp     = erl_element(2, msg);
  host    = erl_iolist_to_string(tmp);
  erl_free_term(tmp);

  tmp     = erl_element(3, msg);
  port    = ERL_INT_VALUE(tmp);
  erl_free_term(tmp);

  tmp     = erl_element(4, msg);
  db_name = erl_iolist_to_string(tmp);
  erl_free_term(tmp);

  tmp     = erl_element(5, msg);
  user    = erl_iolist_to_string(tmp);
  erl_free_term(tmp);

  tmp     = erl_element(6, msg);
  passwd  = erl_iolist_to_string(tmp);
  erl_free_term(tmp);

  /* TODO: handle options, passed in next. */

  logmsg("INFO: Connecting to %s on %s:%d as %s", db_name, host, port, user);
  if (mysql_real_connect(&dbh, host, user, passwd,
                         db_name, port, NULL, 0) == NULL) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_errno(&dbh), mysql_error(&dbh));
    write_msg(resp);
    erl_free_term(resp);
    exit(2);
  }

  resp = erl_format("ok");
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_query(ETERM *cmd)
{
  ETERM *query, *resp;
  MYSQL_STMT *handle;
  char *q;

  query = erl_element(2, cmd);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  logmsg("INFO: got query: %s", q);

  handle = mysql_stmt_init(&dbh);
  if (mysql_stmt_prepare(handle, q, strlen(q))) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_stmt_errno(handle), mysql_stmt_error(handle));
  } else if (mysql_stmt_execute(handle)) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_stmt_errno(handle), mysql_stmt_error(handle));
  } else {
    set_mysql_results(handle);
    if (results) {
      resp = handle_mysql_result();
    } else {
      if (mysql_num_fields(results) == 0)
        resp = erl_format("{updated, ~i}", numrows);
      else
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_stmt_errno(handle), mysql_stmt_error(handle));
    }
  }

  erl_free(q);
  mysql_stmt_close(handle);

  write_msg(resp);
  erl_free_term(resp);
}

int
bind_string(MYSQL_BIND *bind, const ETERM *erl_value, unsigned long len)
{
  char *val;
  unsigned long slen;

  val = erl_iolist_to_string(erl_value);
  if (!val) {
    logmsg("ERROR: bind_string val is NULL");
    return -1;
  }

  slen = strlen(val);

  bind->buffer_type = MYSQL_TYPE_BLOB;
  bind->buffer_length = len;

  bind->length = safe_malloc(sizeof(unsigned long));
  memcpy(bind->length, &slen, sizeof(unsigned long));

  bind->buffer = safe_malloc((slen + 1) * sizeof(char));
  memcpy(bind->buffer, val, slen);

  free(val);
  return 0;
}

/*
 * http://dev.mysql.com/doc/refman/5.1/en/mysql-stmt-execute.html
 *
 * 6 >  odbc:param_query(Ref,
 *                       "INSERT INTO EMPLOYEE (NR, FIRSTNAME, "
 *                       "LASTNAME, GENDER) VALUES(?, ?, ?, ?)",
 *                       [{sql_integer,[2,3,4,5,6,7,8]},
 *                        {{sql_varchar, 20},
 *                         ["John", "Monica", "Ross", "Rachel",
 *                          "Piper", "Prue", "Louise"]},
 *                        {{sql_varchar, 20},
 *                         ["Doe","Geller","Geller", "Green",
 *                          "Halliwell", "Halliwell", "Lane"]},
 *                        {{sql_char, 1}, ["M","F","M","F","T","F","F"]}]).
 * {updated, 7}
 */
void
handle_param_query(ETERM *msg)
{
  ETERM *query, *params, *p, *tmp, *resp;
  MYSQL_STMT *handle;
  MYSQL_BIND *bind;
  char *q;
  int param_count, i;

  query = erl_element(2, msg);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  logmsg("INFO: got param query: %s", q);

  params = erl_element(3, msg);
  erl_free_term(params);

  handle = mysql_stmt_init(&dbh);
  if (mysql_stmt_prepare(handle, q, strlen(q))) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_stmt_errno(handle), mysql_stmt_error(handle));
  } else {
    param_count = mysql_stmt_param_count(handle);
    if (param_count != erl_length(params)) {
      resp = erl_format("{error, {mysql_error, -1, [expected_params, %d, got_params, %d]}}", param_count, erl_length(params));
    } else {
      bind = safe_malloc(param_count * sizeof(MYSQL_BIND));
      memset(bind, 0, param_count * sizeof(MYSQL_BIND));

      for (i = 0, tmp = params;
           i < param_count && (p = erl_hd(tmp)) != NULL;
           i++, tmp = erl_tl(tmp)) {
        ETERM *type, *value;

        type = erl_element(1, p);
        value = erl_element(2, p);

        if (ERL_IS_TUPLE(type)) {
          // Parameter Type + Size: {Type, Size}
          ETERM *t_type, *t_size;
          char *t;
          unsigned long size;

          t_size = erl_element(2, type);
          size = ERL_INT_VALUE(t_size);
          bind[i].buffer_length = size;
          erl_free_term(t_size);

          t_type = erl_element(1, type);
          t = (char *)ERL_ATOM_PTR(t_type);
          bind[i].length = safe_malloc(sizeof(unsigned long));
          if (strncmp(t, NUMERIC_SQL, strlen(NUMERIC_SQL)) == 0) {
            int val;

            bind[i].buffer_type = MYSQL_TYPE_LONG;
            *bind[i].length = sizeof(int);
            bind[i].buffer = safe_malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = ERL_INT_VALUE(value);
            memcpy(bind[i].buffer, &val, *bind[i].length);
          } else if (strncmp(t, DECIMAL_SQL, strlen(DECIMAL_SQL)) == 0) {
            char *val;

            bind[i].buffer_type = MYSQL_TYPE_STRING;
            *bind[i].length = bind[i].buffer_length * sizeof(char);
            bind[i].buffer = safe_malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = erl_iolist_to_string(value);
            if (val) {
              memcpy(bind[i].buffer, val, *bind[i].length);
              free(val);
            }
          } else if (strncmp(t, FLOAT_SQL, strlen(FLOAT_SQL)) == 0) {
            float val;

            bind[i].buffer_type = MYSQL_TYPE_FLOAT;
            *bind[i].length = sizeof(float);
            bind[i].buffer = safe_malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = ERL_FLOAT_VALUE(value);
            memcpy(bind[i].buffer, &val, *bind[i].length);
          } else if (strncmp(t, CHAR_SQL, strlen(CHAR_SQL)) == 0) {
            char *val;

            bind[i].buffer_type = MYSQL_TYPE_STRING;
            *bind[i].length = bind[i].buffer_length * sizeof(char);
            bind[i].buffer = safe_malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = erl_iolist_to_string(value);
            if (val) {
              memcpy(bind[i].buffer, val, *bind[i].length);
              free(val);
            }
          } else if (strncmp(t, VARCHAR_SQL, strlen(VARCHAR_SQL)) == 0) {
            (void)bind_string(&bind[i], value, size);
          } else {
            ETERM *resp;

            resp = erl_format("{error, {unknown_sized_type, ~s, ~i}}",
                              t, bind[i].buffer_length);
            write_msg(resp);
            erl_free_term(resp);
          }
          erl_free_term(t_type);
        } else {
          char *t;

          t = (char *)ERL_ATOM_PTR(type);
          if (strncmp(t, TIMESTAMP_SQL, strlen(TIMESTAMP_SQL)) == 0) {
            bind[i].buffer_type = MYSQL_TYPE_TIMESTAMP;
            *bind[i].length = sizeof(MYSQL_TIME);
            bind[i].buffer = safe_malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            memcpy(bind[i].buffer, value, *bind[i].length);
          } else if (strncmp(t, INTEGER_SQL, strlen(INTEGER_SQL)) == 0) {
            int val;

            bind[i].buffer_type = MYSQL_TYPE_LONG;
            *bind[i].length = sizeof(int);
            bind[i].buffer = safe_malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = ERL_INT_VALUE(value);
            memcpy(bind[i].buffer, &val, *bind[i].length);
          } else {
            ETERM *resp;

            resp = erl_format("{error, {unknown_type, ~s}}", t);
            write_msg(resp);
            erl_free_term(resp);
          }
        }

        if (ERL_IS_ATOM(value)
            && strncmp((char *)ERL_ATOM_PTR(value),
                       NULL_SQL, strlen(NULL_SQL)) == 0)
          bind[i].is_null = &TRUTHY;
        else
          bind[i].is_null = &FALSY;

        erl_free_term(value);
        erl_free_term(type);
      }
      erl_free_term(params);

      if (mysql_stmt_bind_param(handle, bind)) {
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_stmt_errno(handle), mysql_stmt_error(handle));
      } else {
        if (mysql_stmt_execute(handle)) {
          resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                            mysql_stmt_errno(handle), mysql_stmt_error(handle));
        } else {
          set_mysql_results(handle);
          if (results) {
            resp = handle_mysql_result();
          } else {
            if (mysql_num_fields(results) == 0)
              resp = erl_format("{updated, ~i}", numrows);
            else
              resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                                mysql_stmt_errno(handle), mysql_stmt_error(handle));
          }
        }
      }

      for (i = 0; i < param_count; i++) {
        free(bind[i].length);
        free(bind[i].buffer);
      }
      free(bind);
    }
  }
  erl_free(q);

  mysql_stmt_close(handle);

  write_msg(resp);
  erl_free_term(resp);
}

void
handle_select_count(ETERM *msg)
{
  ETERM *query, *resp;
  MYSQL_STMT *handle;
  char *q;

  query = erl_element(2, msg);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  logmsg("INFO: Got select count for: %s", q);

  handle = mysql_stmt_init(&dbh);
  if (mysql_stmt_prepare(handle, q, strlen(q))) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_stmt_errno(handle), mysql_stmt_error(handle));
  } else if (mysql_stmt_execute(handle)) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_stmt_errno(handle), mysql_stmt_error(handle));
  } else {
    set_mysql_results(handle);
    if (results) {
      resp = erl_format("{ok, ~i}", mysql_stmt_affected_rows(handle));
    } else if (mysql_num_fields(results) == 0) {
      resp = erl_format("{ok, ~i}", mysql_stmt_affected_rows(handle));
    } else {
      resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                        mysql_stmt_errno(handle), mysql_stmt_error(handle));
    }
  }
  erl_free(q);

  write_msg(resp);
  erl_free_term(resp);
}

void
handle_first(ETERM *msg)
{
  ETERM *ecols, *erows, *resp;

  if (results == NULL) {
    resp = erl_format("{error, result_set_does_not_exist}");
    write_msg(resp);
    erl_free_term(resp);

    return;
  }

  mysql_stmt_data_seek(sth, resultoffset);
  resultoffset = 1;

  ecols = make_cols();
  erows = make_rows(1);
  resp = erl_format("{selected, ~w, ~w}", ecols, erows);
  erl_free_term(erows);

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_last(ETERM *msg)
{
  ETERM *ecols, *erows, *resp;

  if (results == NULL) {
    resp = erl_format("{error, result_set_does_not_exist}");
    write_msg(resp);
    erl_free_term(resp);

    return;
  }

  mysql_stmt_data_seek(sth, numrows - 1);
  resultoffset = numrows;

  ecols = make_cols();
  erows = make_rows(1);
  resp = erl_format("{selected, ~w, ~w}", ecols, erows);
  erl_free_term(erows);

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_next(ETERM *msg)
{
  ETERM *ecols, *erows, *resp;

  if (results == NULL) {
    resp = erl_format("{error, result_set_does_not_exist}");
    write_msg(resp);
    erl_free_term(resp);

    return;
  }

  ecols = make_cols();
  if (resultoffset == numrows) {
    resp = erl_format("{selected, ~w, []}", ecols);
  } else {
    mysql_stmt_data_seek(sth, resultoffset);
    resultoffset++;
    erows = make_rows(1);
    resp = erl_format("{selected, ~w, ~w}", ecols, erows);
    erl_free_term(erows);
  }

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_prev(ETERM *msg)
{
  ETERM *ecols, *erows, *resp;

  if (results == NULL) {
    resp = erl_format("{error, result_set_does_not_exist}");
    write_msg(resp);
    erl_free_term(resp);

    return;
  }

  ecols = make_cols();
  if (resultoffset <= 1) {
    resp = erl_format("{selected, ~w, []}", ecols);
  } else {
    resultoffset--;
    mysql_stmt_data_seek(sth, resultoffset - 1);
    erows = make_rows(1);
    resp = erl_format("{selected, ~w, ~w}", ecols, erows);
    erl_free_term(erows);
  }

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_select(ETERM *msg)
{
  ETERM *epos, *ecount, *ecols, *erows, *resp;
  my_ulonglong pos, count;

  epos   = erl_element(2, msg);
  ecount = erl_element(3, msg);
  pos    = ERL_INT_UVALUE(epos);
  count  = ERL_INT_UVALUE(ecount);
  erl_free_term(epos);
  erl_free_term(ecount);

  if (results == NULL) {
    resp = erl_format("{error, result_set_does_not_exist}");
    write_msg(resp);
    erl_free_term(resp);

    return;
  }

  if (ERL_IS_TUPLE(epos)) {
    char *pos_type;
    unsigned int pos_count;

    pos_type = ERL_ATOM_PTR(erl_element(1, epos));
    pos_count = ERL_INT_UVALUE(erl_element(2, epos));
    if (strncmp(pos_type, SELECT_ABSOLUTE, strlen(SELECT_ABSOLUTE)) == 0) {
      resultoffset = pos_count - 1;
    } else if (strncmp(pos_type, SELECT_RELATIVE, strlen(SELECT_RELATIVE)) == 0) {
      resultoffset += pos_count - 1;
    } else {
      resp = erl_format("{error, unknown_position, ~w}", epos);
      write_msg(resp);
      erl_free_term(resp);

      return;
    }
  } else {
    if (strncmp((char *)ERL_ATOM_PTR(epos), SELECT_NEXT, strlen(SELECT_NEXT)) == 0) {
      handle_next(NULL);
      return;
    } else {
      resp = erl_format("{error, unknown_position, ~w}", epos);
      write_msg(resp);
      erl_free_term(resp);

      return;
    }
  }

  mysql_stmt_data_seek(sth, resultoffset);

  ecols = make_cols();
  erows = make_rows(count);
  resultoffset += count;
  resp = erl_format("{selected, ~w, ~w}", ecols, erows);
  erl_free_term(erows);

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
dispatch_db_cmd(ETERM *msg)
{
  ETERM *tag;
  char *tag_name;

  tag = erl_element(1, msg);
  tag_name = (char *)ERL_ATOM_PTR(tag);
  if (strncmp(tag_name, CONNECT_MSG, strlen(CONNECT_MSG)) == 0)
    handle_connect(msg);
  else if (strncmp(tag_name, QUERY_MSG, strlen(QUERY_MSG)) == 0)
    handle_query(msg);
  else if (strncmp(tag_name, PARAM_QUERY_MSG, strlen(PARAM_QUERY_MSG)) == 0)
    handle_param_query(msg);
  else if (strncmp(tag_name, SELECT_COUNT_MSG, strlen(SELECT_COUNT_MSG)) == 0)
    handle_select_count(msg);
  else if (strncmp(tag_name, SELECT_MSG, strlen(SELECT_MSG)) == 0)
    handle_select(msg);
  else if (strncmp(tag_name, FIRST_MSG, strlen(FIRST_MSG)) == 0)
    handle_first(msg);
  else if (strncmp(tag_name, LAST_MSG, strlen(LAST_MSG)) == 0)
    handle_last(msg);
  else if (strncmp(tag_name, NEXT_MSG, strlen(NEXT_MSG)) == 0)
    handle_next(msg);
  else if (strncmp(tag_name, PREV_MSG, strlen(PREV_MSG)) == 0)
    handle_prev(msg);
  else {
    ETERM *resp;

    resp = erl_format("{error, {uknown_message, ~s}}", tag);
    write_msg(resp);
    erl_free_term(resp);
  }

  erl_free_term(tag);
}

int
main(int argc, char *argv[])
{
  ETERM *msg;

  openlog();
  logmsg("INFO: starting up.");
  erl_init(NULL, 0);

  mysql_init(&dbh);
  while ((msg = read_msg()) != NULL) {
    dispatch_db_cmd(msg);
    erl_free_term(msg);
  }
  mysql_close(&dbh);

  logmsg("INFO: shutting down.");
  closelog();

  return 0;
}
