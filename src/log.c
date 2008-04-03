/*
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include "log.h"

#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>

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
  char timebuf[32] = "\0";
  struct tm now_tm;
  time_t now_time;
  va_list args;

  va_start(args, format);

  if (logfile == NULL)
    out = stderr;

  if (time(&now_time) == (time_t)-1) {
    (void)fprintf(out, "LOGERROR - Failed to fetch time: ");
  } else {
    (void)localtime_r(&now_time, &now_tm);
    if (strftime(timebuf, sizeof(timebuf), "%Y%m%d %H:%M:%S ", &now_tm) == 0) {
      (void)fprintf(out, "LOGERROR - Failed to parse time (now: %d): ",
                    (int)now_time);
    } else {
      (void)fprintf(out, timebuf);
    }
  }
  (void)fprintf(out, "[%d]: ", getpid());
  (void)vfprintf(out, format, args);
  (void)fprintf(out, "\n");

  fflush(out);

  va_end(args);
}

