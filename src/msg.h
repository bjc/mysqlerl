#ifndef _MSG_H
#define _MSG_H

#include <erl_interface.h>
#include <ei.h>
#include <stdlib.h>

typedef u_int32_t msglen_t;

ETERM *read_msg();
int write_msg(ETERM *msg);

#endif
