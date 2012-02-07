/*
 * MySQL port driver.
 *
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include "io.h"
#include "log.h"
#include "msg.h"

#include <errno.h>
#include <mysql.h>
#include <string.h>

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

my_bool TRUTHY = 1;
my_bool FALSY  = 0;

MYSQL dbh;
MYSQL_RES *results = NULL;
my_ulonglong resultoffset = 0, numrows = 0;

void
set_mysql_results()
{
  if (results)
    mysql_free_result(results);
  results = mysql_store_result(&dbh);

  resultoffset = 0;
  numrows = results ? mysql_num_rows(results) : 0;
}

ETERM *
make_cols(MYSQL_FIELD *fields, unsigned int num_fields)
{
  ETERM **cols, *rc;
  unsigned int i;

  cols = (ETERM **)malloc(num_fields * sizeof(ETERM *));
  if (cols == NULL) {
    logmsg("ERROR: Couldn't allocate %d bytes for columns: %s",
           strerror(errno));
    exit(3);
  }

  for (i = 0; i < num_fields; i++)
    cols[i] = erl_mk_string(fields[i].name);

  rc = erl_mk_list(cols, num_fields);

  for (i = 0; i < num_fields; i++)
    erl_free_term(cols[i]);
  free(cols);

  return rc;
}

ETERM *
make_row(MYSQL_ROW row, unsigned long *lengths, unsigned int num_fields)
{
  ETERM **rowtup, *rc;
  unsigned int i;

  rowtup = (ETERM **)malloc(num_fields * sizeof(ETERM *));
  if (rowtup == NULL) {
    logmsg("ERROR: Couldn't allocate %d bytes for row: %s",
           strerror(errno));
    exit(3);
  }

  for (i = 0; i < num_fields; i++) {
    if (row[i])
      rowtup[i] = erl_mk_estring(row[i], lengths[i]);
    else
      rowtup[i] = erl_mk_atom("null");
  }

  rc = erl_mk_tuple(rowtup, num_fields);
  if (rc == NULL) {
    logmsg("ERROR: couldn't allocate %d-tuple", num_fields);
    exit(3);
  }

  for (i = 0; i < num_fields; i++)
    erl_free_term(rowtup[i]);
  free(rowtup);

  return rc;
}

ETERM *
make_rows(unsigned int num_rows, unsigned int num_fields)
{
  ETERM **rows, *rc;
  unsigned int i;

  rows = (ETERM **)malloc(num_rows * sizeof(ETERM *));
  if (rows == NULL) {
    logmsg("ERROR: Couldn't allocate %d bytes for rows: %s",
           strerror(errno));
    exit(3);
  }

  for (i = 0; i < num_rows; i++) {
    ETERM *rt;
    unsigned long *lengths;
    MYSQL_ROW row;

    row = mysql_fetch_row(results);
    resultoffset++;
    lengths = mysql_fetch_lengths(results);

    rt = make_row(row, lengths, num_fields);
    rows[i] = erl_format("~w", rt);
    erl_free_term(rt);
  }

  rc = erl_mk_list(rows, num_rows);

  for (i = 0; i < num_rows; i++)
    erl_free_term(rows[i]);
  free(rows);

  return rc;
}

ETERM *
handle_mysql_result()
{
  MYSQL_FIELD *fields;
  ETERM *ecols, *erows, *resp;
  unsigned int num_fields;

  num_fields = mysql_num_fields(results);
  fields     = mysql_fetch_fields(results);

  ecols = make_cols(fields, num_fields);
  erows = make_rows(numrows, num_fields);

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
    logmsg("ERROR: Failed to connect to database %s as %s: %s.",
           db_name, user, mysql_error(&dbh));
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
  char *q;

  query = erl_element(2, cmd);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  logmsg("DEBUG: got query msg: %s.", q);
  if (mysql_query(&dbh, q)) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_errno(&dbh), mysql_error(&dbh));
  } else {
    set_mysql_results();
    if (results) {
      resp = handle_mysql_result();
    } else {
      if (mysql_field_count(&dbh) == 0)
        resp = erl_format("{updated, ~i}", mysql_affected_rows(&dbh));
      else
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_errno(&dbh), mysql_error(&dbh));
    }
  }
  erl_free(q);

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
    logmsg("DEBUG: bind_string val is NULL");
    return -1;
  }

  slen = strlen(val);
  logmsg("DEBUG: Storing BLOB(%lu) %s(%lu)", len, val, slen);

  bind->buffer_type = MYSQL_TYPE_BLOB;
  bind->buffer_length = len;

  bind->length = malloc(sizeof(unsigned long));
  memcpy(bind->length, &slen, sizeof(unsigned long));

  bind->buffer = malloc((slen + 1) * sizeof(char));
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
  MYSQL_STMT *sth;
  MYSQL_BIND *bind;
  char *q;
  int param_count, i;

  query = erl_element(2, msg);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  params = erl_element(3, msg);
  erl_free_term(params);

  logmsg("DEBUG: got param query msg: %s.", q);

  sth = mysql_stmt_init(&dbh);
  if (mysql_stmt_prepare(sth, q, strlen(q))) {
    logmsg("DEBUG: couldn't prepare statement.");
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_errno(&dbh), mysql_error(&dbh));
  } else {
    param_count = mysql_stmt_param_count(sth);
    logmsg("DEBUG: expected_count: %d, got_count: %d", param_count, erl_length(params));
    if (param_count != erl_length(params)) {
      resp = erl_format("{error, {mysql_error, -1, [expected_params, %d, got_params, %d]}}", param_count, erl_length(params));
    } else {
      bind = malloc(param_count * sizeof(MYSQL_BIND));
      if (bind == NULL) {
        logmsg("ERROR: Couldn't allocate %d bytes for bind params.",
               param_count * sizeof(MYSQL_BIND));
        exit(3);
      }
      memset(bind, 0, param_count * sizeof(MYSQL_BIND));

      for (i = 0, tmp = params;
           (p = erl_hd(tmp)) != NULL && i < 1000;
           i++, tmp = erl_tl(tmp)) {
        ETERM *type, *value;

        type = erl_element(1, p);
        value = erl_element(2, p);

        if (ERL_IS_TUPLE(type)) {
	  // Parameter Type + Size: {Type, Size}
          ETERM *t_type, *t_size;
          char *t;
	  unsigned long size;

	  logmsg("DEBUG: got tuple param no. %d.", i);
          t_size = erl_element(2, type);
	  size = ERL_INT_VALUE(t_size);
          bind[i].buffer_length = size;
          erl_free_term(t_size);

          t_type = erl_element(1, type);
          t = (char *)ERL_ATOM_PTR(t_type);
          bind[i].length = malloc(sizeof(unsigned long));
          if (strncmp(t, NUMERIC_SQL, strlen(NUMERIC_SQL)) == 0) {
            int val;

	    logmsg("DEBUG: param is numeric");
            bind[i].buffer_type = MYSQL_TYPE_LONG;
            *bind[i].length = sizeof(int);
            bind[i].buffer = malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = ERL_INT_VALUE(value);
            memcpy(bind[i].buffer, &val, *bind[i].length);
          } else if (strncmp(t, DECIMAL_SQL, strlen(DECIMAL_SQL)) == 0) {
            char *val;

	    logmsg("DEBUG: param is decimal");
            bind[i].buffer_type = MYSQL_TYPE_STRING;
            *bind[i].length = bind[i].buffer_length * sizeof(char);
            bind[i].buffer = malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = erl_iolist_to_string(value);
            if (val) {
              memcpy(bind[i].buffer, val, *bind[i].length);
              free(val);
            }
          } else if (strncmp(t, FLOAT_SQL, strlen(FLOAT_SQL)) == 0) {
            float val;

	    logmsg("DEBUG: param is float");
            bind[i].buffer_type = MYSQL_TYPE_FLOAT;
            *bind[i].length = sizeof(float);
            bind[i].buffer = malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = ERL_FLOAT_VALUE(value);
            memcpy(bind[i].buffer, &val, *bind[i].length);
          } else if (strncmp(t, CHAR_SQL, strlen(CHAR_SQL)) == 0) {
            char *val;

	    logmsg("DEBUG: param is string");
            bind[i].buffer_type = MYSQL_TYPE_STRING;
            *bind[i].length = bind[i].buffer_length * sizeof(char);
            bind[i].buffer = malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = erl_iolist_to_string(value);
            if (val) {
              memcpy(bind[i].buffer, val, *bind[i].length);
              free(val);
            }
          } else if (strncmp(t, VARCHAR_SQL, strlen(VARCHAR_SQL)) == 0) {
	    logmsg("DEBUG: param is varchar");
	    (void)bind_string(&bind[i], value, size);
          } else {
            logmsg("ERROR: Unknown sized type: {%s, %d}", t,
                   bind[i].buffer_length);
            exit(3);
          }
          erl_free_term(t_type);
        } else {
          char *t;

          t = (char *)ERL_ATOM_PTR(type);
          if (strncmp(t, TIMESTAMP_SQL, strlen(TIMESTAMP_SQL)) == 0) {
	    logmsg("DEBUG: got timestamp param.");
            bind[i].buffer_type = MYSQL_TYPE_TIMESTAMP;
            *bind[i].length = sizeof(MYSQL_TIME);
            bind[i].buffer = malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            memcpy(bind[i].buffer, value, *bind[i].length);
          } else if (strncmp(t, INTEGER_SQL, strlen(INTEGER_SQL)) == 0) {
	    logmsg("DEBUG: got integer param.");
            int val;

            bind[i].buffer_type = MYSQL_TYPE_LONG;
            *bind[i].length = sizeof(int);
            bind[i].buffer = malloc(*bind[i].length);
            memset(bind[i].buffer, 0, *bind[i].length);

            val = ERL_INT_VALUE(value);
            memcpy(bind[i].buffer, &val, *bind[i].length);
          } else {
            logmsg("ERROR: Unknown type: %s", t);
            exit(3);
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

      logmsg("DEBUG: binding params");
      if (mysql_stmt_bind_param(sth, bind)) {
	logmsg("DEBUG: failed binding params");
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_errno(&dbh), mysql_error(&dbh));
      } else {
	logmsg("DEBUG: preparing cursor");
	unsigned long stmt_type = CURSOR_TYPE_READ_ONLY;
	unsigned long prefetch_rows = 5;
	mysql_stmt_attr_set(sth, STMT_ATTR_CURSOR_TYPE, &stmt_type);
	mysql_stmt_attr_set(sth, STMT_ATTR_PREFETCH_ROWS, &prefetch_rows);

	logmsg("DEBUG: executing statement");
        if (mysql_stmt_execute(sth)) {
	  logmsg("DEBUG: failed executing statement");
          resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                            mysql_errno(&dbh), mysql_error(&dbh));
        } else {
          set_mysql_results();
          if (results) {
            resp = handle_mysql_result();
          } else {
	    logmsg("DEBUG: field count: %d", mysql_field_count(&dbh));
	    logmsg("DEBUG: affected rows: %d", mysql_affected_rows(&dbh));
            if (mysql_field_count(&dbh) == 0)
              resp = erl_format("{updated, ~i}", mysql_affected_rows(&dbh));
            else
              resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                                mysql_errno(&dbh), mysql_error(&dbh));
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

  mysql_stmt_close(sth);

  write_msg(resp);
  erl_free_term(resp);
}

void
handle_select_count(ETERM *msg)
{
  ETERM *query, *resp;
  char *q;

  query = erl_element(2, msg);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  logmsg("DEBUG: got select count msg: %s.", q);
  if (mysql_query(&dbh, q)) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_errno(&dbh), mysql_error(&dbh));
  } else {
    set_mysql_results();
    if (results) {
      resp = erl_format("{ok, ~i}", numrows);
    } else {
      if (mysql_field_count(&dbh) == 0)
        resp = erl_format("{ok, ~i}", mysql_affected_rows(&dbh));
      else
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_errno(&dbh), mysql_error(&dbh));
    }
  }
  erl_free(q);

  write_msg(resp);
  erl_free_term(resp);
}

void
handle_select(ETERM *msg)
{
  MYSQL_FIELD *fields;
  ETERM *epos, *enum_items, *ecols, *erows, *resp;
  my_ulonglong pos, num_items;
  unsigned int num_fields;
  
  epos       = erl_element(2, msg);
  enum_items = erl_element(3, msg);
  pos        = ERL_INT_UVALUE(epos);
  num_items  = ERL_INT_UVALUE(enum_items);
  erl_free_term(enum_items);
  erl_free_term(epos);

  logmsg("DEBUG: got select pos: %d, n: %d.", erl_size(msg), pos, num_items);
  if (results == NULL) {
    logmsg("ERROR: select message w/o cursor.");
    exit(2);
  }

  num_fields   = mysql_num_fields(results);
  fields       = mysql_fetch_fields(results);
  if (resultoffset > 0)
    resultoffset = pos - 1;
  if (num_items > numrows - resultoffset)
    num_items = numrows - resultoffset;
  mysql_data_seek(results, resultoffset);

  ecols = make_cols(fields, num_fields);
  erows = make_rows(num_items, num_fields);
  resp = erl_format("{selected, ~w, ~w}", ecols, erows);
  erl_free_term(erows);

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_first(ETERM *msg)
{
  MYSQL_FIELD *fields;
  ETERM *ecols, *erows, *resp;
  unsigned int num_fields;

  logmsg("DEBUG: got first msg.");
  if (results == NULL) {
    logmsg("ERROR: got first message w/o cursor.");
    exit(2);
  }

  num_fields   = mysql_num_fields(results);
  fields       = mysql_fetch_fields(results);
  resultoffset = 0;
  mysql_data_seek(results, resultoffset);

  ecols = make_cols(fields, num_fields);
  erows = make_rows(1, num_fields);
  resp = erl_format("{selected, ~w, ~w}", ecols, erows);
  erl_free_term(erows);

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_last(ETERM *msg)
{
  MYSQL_FIELD *fields;
  ETERM *ecols, *erows, *resp;
  unsigned int num_fields;

  logmsg("DEBUG: got last msg.");
  if (results == NULL) {
    logmsg("ERROR: got last message w/o cursor.");
    exit(2);
  }

  num_fields   = mysql_num_fields(results);
  fields       = mysql_fetch_fields(results);
  resultoffset = numrows - 1;
  mysql_data_seek(results, resultoffset);

  ecols = make_cols(fields, num_fields);
  erows = make_rows(1, num_fields);
  resp = erl_format("{selected, ~w, ~w}", ecols, erows);
  erl_free_term(erows);

  erl_free_term(ecols);
  write_msg(resp);
  erl_free_term(resp);
}

void
handle_next(ETERM *msg)
{
  MYSQL_FIELD *fields;
  ETERM *ecols, *erows, *resp;
  unsigned int num_fields;

  logmsg("DEBUG: got next msg.");
  if (results == NULL) {
    logmsg("ERROR: got next message w/o cursor.");
    exit(2);
  }

  num_fields = mysql_num_fields(results);
  fields     = mysql_fetch_fields(results);

  ecols = make_cols(fields, num_fields);
  logmsg("resultoffset: %d, num_rows: %d", resultoffset, numrows);
  if (resultoffset == numrows) {
    resp = erl_format("{selected, ~w, []}", ecols);
  } else {
    erows = make_rows(1, num_fields);
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
  MYSQL_FIELD *fields;
  ETERM *ecols, *erows, *resp;
  unsigned int num_fields;

  logmsg("DEBUG: got prev msg.");
  if (results == NULL) {
    logmsg("ERROR: got prev message w/o cursor.");
    exit(2);
  }

  num_fields = mysql_num_fields(results);
  fields     = mysql_fetch_fields(results);

  ecols = make_cols(fields, num_fields);
  logmsg("resultoffset: %d, num_rows: %d", resultoffset, numrows);
  if (resultoffset <= 1) {
    resp = erl_format("{selected, ~w, []}", ecols);
  } else {
    resultoffset = resultoffset - 1;
    mysql_data_seek(results, resultoffset);
    erows = make_rows(1, num_fields);

    /* Rewind to position at the point we returned. */
    resultoffset = resultoffset - 1;
    mysql_data_seek(results, resultoffset);
    resp = erl_format("{selected, ~w, ~w}", ecols, erows);
    erl_free_term(erows);
  }

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
    logmsg("WARNING: message type %s unknown.", (char *)ERL_ATOM_PTR(tag));
    erl_free_term(tag);
    exit(3);
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
