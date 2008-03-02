/*
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include "log.h"

#include <stdio.h>
#include <stdarg.h>

const char *LOGPATH = "/tmp/mysqlerl.log";
static FILE *logfile = NULL;

void
openlog()
{
  logfile = fopen(LOGPATH, "a");
}

void
closelog()
{
  fclose(logfile);
}

void
logmsg(const char *format, ...)
{
  FILE *out = logfile;
  va_list args;

  if (logfile == NULL)
    out = stderr;

  va_start(args, format);
  (void)vfprintf(out, format, args);
  (void)fprintf(out, "\n");
  va_end(args);

  fflush(out);
}

