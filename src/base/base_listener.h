/*
 * base_listener.h
 *
 *  Created on: 2016-3-16
 *      Author: ziteng
 */

#ifndef __BASE_BASE_LISTENER_H__
#define __BASE_BASE_LISTENER_H__

#include "util.h"
#include "base_socket.h"

template <class T>
void connect_callback(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
	if (msg == NETLIB_MSG_CONNECT) {
		T* pConn = new T();
		pConn->OnConnect((BaseSocket*)pParam);
	} else {
		printf("!!!error msg: %d\n", msg);
	}
}

template <class T>
int start_listen(const string& server_ip, uint16_t port)
{
    BaseSocket* base_socket = new BaseSocket();
    assert(base_socket);
    net_handle_t handle = base_socket->Listen(server_ip.c_str(), port, connect_callback<T>, NULL);
    if (handle == NETLIB_INVALID_HANDLE) {
        return 1;
    } else {
        return 0;
    }
}

#endif
