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

const char *QUERY_MSG = "sql_query";

void
usage()
{
  fprintf(stderr, "Usage: mysqlerl host port db_name user passwd\n");
  exit(1);
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
      rowtup[i] = erl_mk_atom("NULL");
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
make_rows(MYSQL_RES *result, unsigned int num_rows, unsigned int num_fields)
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

    row = mysql_fetch_row(result);
    lengths = mysql_fetch_lengths(result);

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
handle_mysql_result(MYSQL_RES *result)
{
  MYSQL_FIELD *fields;
  ETERM *ecols, *erows, *resp;
  unsigned int num_fields, num_rows;

  num_fields = mysql_num_fields(result);
  fields     = mysql_fetch_fields(result);
  num_rows   = mysql_num_rows(result);

  ecols = make_cols(fields, num_fields);
  erows = make_rows(result, num_rows, num_fields);

  resp = erl_format("{selected, ~w, ~w}", ecols, erows);

  erl_free_term(ecols);
  erl_free_term(erows);

  return resp;
}

void
handle_sql_query(MYSQL *dbh, ETERM *cmd)
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
    MYSQL_RES *result;

    result = mysql_store_result(dbh);
    if (result) {
      resp = handle_mysql_result(result);
      mysql_free_result(result);
    } else {
      if (mysql_field_count(dbh) == 0)
        resp = erl_format("{num_rows, ~i}", mysql_affected_rows(dbh));
      else
        resp = erl_format("{error, {mysql_error, ~i, ~s}}",
                          mysql_errno(dbh), mysql_error(dbh));
    }
  }
  erl_free(q);

  logmsg("DEBUG: prepping buffers and sending.");
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
    handle_sql_query(dbh, msg);
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
    logmsg("ERROR: Failed to connect to database %s: %s (%s:%s).",
           db_name, mysql_error(&dbh), user, passwd);
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
