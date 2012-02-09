/*
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#ifndef _IO_H
#define _IO_H

#include <sys/types.h>

int restartable_read(unsigned char *buf, size_t buflen);
int restartable_write(const unsigned char *buf, size_t buflen);

#endif
