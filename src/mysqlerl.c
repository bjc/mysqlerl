/*
 * MySQL port driver.
 *
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include "io.h"
#include "log.h"
#include "msg.h"

#include <mysql.h>
#include <string.h>

const char *QUERY_MSG = "sql_query";

ETERM *
handle_mysql_result(MYSQL_RES *result)
{
  MYSQL_FIELD *fields;
  unsigned int i, num_fields, num_rows;
  ETERM **cols, *ecols, **rows, *erows, *resp;

  num_fields = mysql_num_fields(result);
  fields = mysql_fetch_fields(result);
  cols = (ETERM **)malloc(num_fields * sizeof(ETERM *));
  for (i = 0; i < num_fields; i++) {
    logmsg("DEBUG: cols[%d]: %s", i, fields[i].name);
    cols[i] = erl_mk_string(fields[i].name);
  }
  ecols = erl_mk_list(cols, num_fields);

  num_rows = mysql_num_rows(result);
  rows = (ETERM **)malloc(num_rows * sizeof(ETERM *));
  for (i = 0; i < num_rows; i++) {
    ETERM **rowtup, *rt;
    unsigned long *lengths;
    MYSQL_ROW row;
    unsigned int j;

    row = mysql_fetch_row(result);
    lengths = mysql_fetch_lengths(result);

    rowtup = (ETERM **)malloc(num_fields * sizeof(ETERM *));
    for (j = 0; j < num_fields; j++) {
      logmsg("DEBUG: rows[%d][%d] (%d): '%s'", i, j, lengths[j], row[j]);
      if (row[j])
        rowtup[j] = erl_mk_estring(row[j], lengths[j]);
      else
        rowtup[j] = erl_mk_atom("NULL");
      logmsg("DEBUG: rowtup[%d]: %d", j, rowtup[j]);
    }
    logmsg("DEBUG: making tuple of %d", num_fields);
    rt = erl_mk_tuple(rowtup, num_fields);
    if (rt == NULL) {
      logmsg("ERROR: couldn't allocate %d-tuple", num_fields);
      exit(3);
    }
    logmsg("DEBUG: copying rt");
    rows[i] = erl_format("~w", rt);

    logmsg("DEBUG: freeing row fields");
    for (j = 0; j < num_fields; j++)
      erl_free_term(rowtup[j]);
    free(rowtup);
    erl_free_term(rt);
  }
  logmsg("DEBUG: making row list");
  erows = erl_mk_list(rows, num_rows);

  logmsg("DEBUG: preparing response");
  resp = erl_format("{selected, ~w, ~w}",
                    ecols, erows);

  for (i = 0; i < num_fields; i++)
    erl_free_term(cols[i]);
  free(cols);
  erl_free_term(ecols);

  for (i = 0; i < num_rows; i++)
    erl_free_term(rows[i]);
  free(rows);
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

void
usage()
{
  fprintf(stderr, "Usage: mysqlerl host port db_name user passwd\n");
  exit(1);
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
