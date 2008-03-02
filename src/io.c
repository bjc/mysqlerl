#include "io.h"
#include "log.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

int
restartable_read(unsigned char *buf, size_t buflen)
{
  ssize_t rc, readb;

  rc = 0;
  READLOOP:
  while (rc < buflen) {
    readb = read(STDIN_FILENO, buf + rc, buflen - rc);
    if (readb == -1) {
      if (errno == EAGAIN || errno == EINTR)
        goto READLOOP;

      return -1;
    } else if (readb == 0) {
      logmsg("ERROR: EOF trying to read additional %d bytes from "
             "standard input", buflen - rc);
      return -1;
    }

    rc += readb;
  }

  return rc;
}

int
restartable_write(const unsigned char *buf, size_t buflen)
{
  ssize_t rc, wroteb;

  rc = 0;
  WRITELOOP:
  while (rc < buflen) {
    wroteb = write(STDOUT_FILENO, buf + rc, buflen - rc);
    if (wroteb == -1) {
      if (errno == EAGAIN || errno == EINTR)
        goto WRITELOOP;

      return -1;
    }

    rc += wroteb;
  }

  return rc;
}
