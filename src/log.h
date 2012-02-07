/*
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#ifndef _LOG_H
#define _LOG_H

void openlog();
void closelog();
void logmsg(const char *format, ...);

#endif
