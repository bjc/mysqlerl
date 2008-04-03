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

const char *QUERY_MSG        = "sql_query";
const char *PARAM_QUERY_MSG  = "sql_param_query";
const char *SELECT_COUNT_MSG = "sql_select_count";
const char *SELECT_MSG       = "sql_select";
const char *FIRST_MSG        = "sql_first";
const char *LAST_MSG         = "sql_last";
const char *NEXT_MSG         = "sql_next";
const char *PREV_MSG         = "sql_prev";

MYSQL_RES *results = NULL;
my_ulonglong resultoffset = 0, numrows = 0;

void
usage()
{
  fprintf(stderr, "Usage: mysqlerl host port db_name user passwd\n");
  exit(1);
}

void
set_mysql_results(MYSQL_RES *res)
{
  if (results)
    mysql_free_result(results);
  results = res;
  resultoffset = 0;
  numrows = mysql_num_rows(results);
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
handle_query(MYSQL *dbh, ETERM *cmd)
{
  ETERM *query, *resp;
  char *q;

  query = erl_element(2, cmd);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  logmsg("DEBUG: got query msg: %s.", q);
  if (mysql_query(dbh, q)) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_errno(dbh), mysql_error(dbh));
  } else {
    set_mysql_results(mysql_store_result(dbh));
    if (results) {
      resp = handle_mysql_result(results);
      set_mysql_results(NULL);
    } else {
      if (mysql_field_count(dbh) == 0)
        resp = erl_format("{updated, ~i}", mysql_affected_rows(dbh));
      else
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_errno(dbh), mysql_error(dbh));
    }
  }
  erl_free(q);

  write_msg(resp);
  erl_free_term(resp);
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
handle_param_query(MYSQL *dbh, ETERM *msg)
{
  ETERM *query, *params;
  char *q;

  query = erl_element(2, msg);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  params = erl_element(3, msg);
  erl_free_term(params);

  logmsg("DEBUG: got param query msg: %s.", q);

  erl_free(q);
}

void
handle_select_count(MYSQL *dbh, ETERM *msg)
{
  ETERM *query, *resp;
  char *q;

  query = erl_element(2, msg);
  q = erl_iolist_to_string(query);
  erl_free_term(query);

  logmsg("DEBUG: got select count msg: %s.", q);
  if (mysql_query(dbh, q)) {
    resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                      mysql_errno(dbh), mysql_error(dbh));
  } else {
    set_mysql_results(mysql_store_result(dbh));
    if (results) {
      resp = erl_format("{ok, ~i}", numrows);
    } else {
      if (mysql_field_count(dbh) == 0)
        resp = erl_format("{ok, ~i}", mysql_affected_rows(dbh));
      else
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_errno(dbh), mysql_error(dbh));
    }
  }
  erl_free(q);

  write_msg(resp);
  erl_free_term(resp);
}

void
handle_select(MYSQL *dbh, ETERM *msg)
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
  resultoffset = pos - 1;
  if (resultoffset < 0)
    resultoffset = 0;
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
handle_first(MYSQL *dbh, ETERM *msg)
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
handle_last(MYSQL *dbh, ETERM *msg)
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
handle_next(MYSQL *dbh, ETERM *msg)
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
handle_prev(MYSQL *dbh, ETERM *msg)
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
  if (resultoffset == 0) {
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
dispatch_db_cmd(MYSQL *dbh, ETERM *msg)
{
  ETERM *tag;

  tag = erl_element(1, msg);
  if (strncmp((char *)ERL_ATOM_PTR(tag),
              QUERY_MSG, strlen(QUERY_MSG)) == 0) {
    handle_query(dbh, msg);
  } else if (strncmp((char *)ERL_ATOM_PTR(tag),
                     PARAM_QUERY_MSG, strlen(PARAM_QUERY_MSG)) == 0) {
    handle_param_query(dbh, msg);
  } else if (strncmp((char *)ERL_ATOM_PTR(tag),
                     SELECT_COUNT_MSG, strlen(SELECT_COUNT_MSG)) == 0) {
    handle_select_count(dbh, msg);
  } else if (strncmp((char *)ERL_ATOM_PTR(tag),
                     SELECT_MSG, strlen(SELECT_MSG)) == 0) {
    handle_select(dbh, msg);
  } else if (strncmp((char *)ERL_ATOM_PTR(tag),
                     FIRST_MSG, strlen(FIRST_MSG)) == 0) {
    handle_first(dbh, msg);
  } else if (strncmp((char *)ERL_ATOM_PTR(tag),
                     LAST_MSG, strlen(LAST_MSG)) == 0) {
    handle_last(dbh, msg);
  } else if (strncmp((char *)ERL_ATOM_PTR(tag),
                     NEXT_MSG, strlen(NEXT_MSG)) == 0) {
    handle_next(dbh, msg);
  } else if (strncmp((char *)ERL_ATOM_PTR(tag),
                     PREV_MSG, strlen(PREV_MSG)) == 0) {
    handle_prev(dbh, msg);
  } else {
    logmsg("WARNING: message type %s unknown.", (char *)ERL_ATOM_PTR(tag));
    erl_free_term(tag);
    exit(3);
  }

  erl_free_term(tag);
}

int
main(int argc, char *argv[])
{
  MYSQL dbh;
  char *host, *port, *db_name, *user, *passwd;
  ETERM *msg;

  openlog();
  logmsg("INFO: starting up.");

  if (argc < 6)
    usage();

  host    = argv[1];
  port    = argv[2];
  db_name = argv[3];
  user    = argv[4];
  passwd  = argv[5];

  erl_init(NULL, 0);

  mysql_init(&dbh);
  if (mysql_real_connect(&dbh, host, user, passwd,
                         db_name, atoi(port), NULL, 0) == NULL) {
    logmsg("ERROR: Failed to connect to database %s as %s: %s.",
           db_name, user, mysql_error(&dbh));
    exit(2);
  }

  while ((msg = read_msg()) != NULL) {
    dispatch_db_cmd(&dbh, msg);
    erl_free_term(msg);
  }

  mysql_close(&dbh);

  logmsg("INFO: shutting down.");
  closelog();

  return 0;
}
