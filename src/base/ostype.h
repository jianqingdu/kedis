/*
 * ostype.h
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#ifndef __BASE_OS_TYPE_H__
#define __BASE_OS_TYPE_H__

#include <sys/time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>

const int SOCKET_ERROR	= -1;
const int INVALID_SOCKET = -1;
const int NETLIB_INVALID_HANDLE = -1;

typedef unsigned char	uchar_t;
typedef int				net_handle_t;

enum
{
	NETLIB_MSG_CONNECT = 1,
	NETLIB_MSG_CONFIRM,
	NETLIB_MSG_READ,
	NETLIB_MSG_WRITE,
	NETLIB_MSG_CLOSE,
	NETLIB_MSG_TIMER,
	NETLIB_MSG_LOOP,
};

typedef void (*callback_t)(void* callback_data, uint8_t msg, uint32_t handle, void* pParam);

#endif
