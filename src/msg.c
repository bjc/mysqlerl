/*
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include "msg.h"

#include "io.h"
#include "log.h"

#include <string.h>

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
  buf = (unsigned char *)malloc(len);
  if (buf == NULL) {
    logmsg("ERROR: Couldn't malloc %d bytes: %s.", len,
           strerror(errno));

    exit(2);
  }

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
  buf = (unsigned char *)malloc(buflen);
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
