/*
 * base_socket.cpp
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#include <assert.h>
#include "base_socket.h"
#include "event_loop.h"
#include "util.h"

// replace socket with handle to notify upper layer, cause socket will be reused, 
// while handle will be increasing whenever a new BaseSocket is created to avoid confusion
static atomic<net_handle_t> g_handle_allocator {1};

BaseSocket::BaseSocket()
{
	//printf("BaseSocket::BaseSocket\n");
	m_socket = INVALID_SOCKET;
	m_state = SOCKET_STATE_IDLE;
	
    do {
        m_handle = g_handle_allocator++;
        if (m_handle < 0) {
            m_handle = 1;
        }
        
        // skip handle that is already used in listen socket, assume connection socket can not last too long
        if (get_main_event_loop()->FindBaseSocket(m_handle) == NULL) {
            break;
        }
    } while (true);
}

BaseSocket::~BaseSocket()
{
	//printf("BaseSocket::~BaseSocket, socket=%d\n", m_socket);
}

net_handle_t BaseSocket::Listen(const char* server_ip, uint16_t port, callback_t callback, void* callback_data)
{
	m_local_ip = server_ip;
	m_local_port = port;
	m_callback = callback;
	m_callback_data = callback_data;

	m_socket = socket(AF_INET, SOCK_STREAM, 0);
	if (m_socket == INVALID_SOCKET) {
		printf("socket failed, err_code=%d\n", errno);
		return NETLIB_INVALID_HANDLE;
	}

	SetReuseAddr(m_socket);
	SetNonblock(m_socket, true);

	sockaddr_in serv_addr;
	SetAddr(server_ip, port, &serv_addr);
    int ret = ::bind(m_socket, (sockaddr*)&serv_addr, sizeof(serv_addr));
	if (ret == SOCKET_ERROR) {
		printf("bind %s:%d failed, err_code=%d\n", server_ip, port, errno);
		close(m_socket);
		return NETLIB_INVALID_HANDLE;
	}

	ret = listen(m_socket, 1024);
	if (ret == SOCKET_ERROR) {
		printf("listen failed, err_code=%d\n", errno);
		close(m_socket);
		return NETLIB_INVALID_HANDLE;
	}
    
    GetBindAddr(m_socket, m_local_ip, m_local_port);

	m_state = SOCKET_STATE_LISTENING;

	printf("BaseSocket::Listen on %s:%d\n", server_ip, port);

    m_event_loop = get_main_event_loop();
	m_event_loop->AddEvent(m_socket, SOCKET_READ | SOCKET_EXCEP | SOCKET_ADD_CONN, this);
    return m_handle;
}

net_handle_t BaseSocket::Connect(const char* server_ip, uint16_t port, callback_t callback, void* callback_data,
                                 EventLoop* event_loop)
{
	printf("BaseSocket::Connect, server_ip=%s, port=%d\n", server_ip, port);

	m_remote_ip = server_ip;
	m_remote_port = port;
	m_callback = callback;
	m_callback_data = callback_data;

	m_socket = socket(AF_INET, SOCK_STREAM, 0);
	if (m_socket == INVALID_SOCKET) {
		printf("socket failed, err_code=%d\n", errno);
		return NETLIB_INVALID_HANDLE;
	}

	SetNonblock(m_socket, true);
	SetNoDelay(m_socket);

	sockaddr_in serv_addr;
	SetAddr(server_ip, port, &serv_addr);
	int ret = connect(m_socket, (sockaddr*)&serv_addr, sizeof(serv_addr));
	if ( (ret == SOCKET_ERROR) && (!_IsBlock(errno)) ) {
		printf("connect failed, err_code=%d\n", errno);
		close(m_socket);
		return NETLIB_INVALID_HANDLE;
	}

    GetBindAddr(m_socket, m_local_ip, m_local_port);
    
	m_state = SOCKET_STATE_CONNECTING;
    
    if (event_loop) {
        m_event_loop = event_loop;
    } else {
        m_event_loop = get_io_event_loop(m_handle);
    }
	m_event_loop->AddEvent(m_socket, SOCKET_ALL|SOCKET_ADD_CONN, this);
	
	return m_handle;
}

int BaseSocket::Send(void* buf, int len)
{
    int ret = (int)send(m_socket, (char*)buf, len, 0);
	if (ret == SOCKET_ERROR) {
		if (_IsBlock(errno)) {
#ifdef __APPLE__
			m_event_loop->AddEvent(m_socket, SOCKET_WRITE, this);
#endif
			ret = 0;
		} else {
			printf("!!!send failed, error code: %d\n", errno);
		}
	}
  
	return ret;
}

int BaseSocket::Recv(void* buf, int len)
{
    int n = (int)recv(m_socket, (char*)buf, len, 0);
    if (n == 0) {
        m_state = SOCKET_STATE_PEER_CLOSING;
    }
    
    return n;
}

int BaseSocket::Close()
{
    m_state = SOCKET_STATE_CLOSING;
	m_event_loop->RemoveEvent(m_socket, SOCKET_ALL|SOCKET_DEL_CONN, this);
	close(m_socket);
    ReleaseRef();
    
	return 0;
}

void BaseSocket::OnConnect()
{
    m_callback(m_callback_data, NETLIB_MSG_CONNECT, m_handle, this);
}

void BaseSocket::OnRead()
{
	if (m_state == SOCKET_STATE_LISTENING) {
		_AcceptNewSocket();
	} else {
		u_long avail = 0;
		if ( (ioctl(m_socket, FIONREAD, &avail) == SOCKET_ERROR) || (avail == 0) ) {
			m_callback(m_callback_data, NETLIB_MSG_CLOSE, m_handle, NULL);
		} else {
			m_callback(m_callback_data, NETLIB_MSG_READ, m_handle, NULL);
            
            // process receive data and FIN packet simultaneously, recv() return 0 means peer close the socket
            if (m_state == SOCKET_STATE_PEER_CLOSING) {
                m_callback(m_callback_data, NETLIB_MSG_CLOSE, m_handle, NULL);
            }
		}
	}
}

void BaseSocket::OnWrite()
{
#ifdef __APPLE__
	m_event_loop->RemoveEvent(m_socket, SOCKET_WRITE, this);
#endif

	if (m_state == SOCKET_STATE_CONNECTING) {
		int error = 0;
		unsigned int len = sizeof(error);
		getsockopt(m_socket, SOL_SOCKET, SO_ERROR, (void*)&error, &len);
		if (error) {
			m_callback(m_callback_data, NETLIB_MSG_CLOSE, m_handle, NULL);
		} else {
			// if the peer and local ip:port is equal, then close the local TCP loop connection
			// see http://www.rampa.sk/static/tcpLoopConnect.html for details
			sockaddr_in local_addr, remote_addr;
			socklen_t local_len, remote_len;
			local_len = remote_len = sizeof(sockaddr_in);

			if (!getsockname(m_socket, (sockaddr*)&local_addr, &local_len) &&
				!getpeername(m_socket, (sockaddr*)&remote_addr, &remote_len) ) {
				if ((local_addr.sin_addr.s_addr == remote_addr.sin_addr.s_addr) &&
					(local_addr.sin_port == remote_addr.sin_port) ) {
					printf("close TCP loop connection\n");
					OnClose();
					return;
				}
			}

            m_state = SOCKET_STATE_CONNECTED;
			m_callback(m_callback_data, NETLIB_MSG_CONFIRM, m_handle, NULL);
		}
	} else if (m_state == SOCKET_STATE_CONNECTED) {
		m_callback(m_callback_data, NETLIB_MSG_WRITE, m_handle, NULL);
	}
}

void BaseSocket::OnClose()
{
    if (m_state == SOCKET_STATE_CLOSING) {
        return;
    }
    
	m_state = SOCKET_STATE_CLOSING;
	m_callback(m_callback_data, NETLIB_MSG_CLOSE, m_handle, NULL);
}

void BaseSocket::OnTimer(uint64_t curr_tick)
{
    if (m_state != SOCKET_STATE_LISTENING) {
        m_callback(m_callback_data, NETLIB_MSG_TIMER, m_handle, &curr_tick);
    }
}

void BaseSocket::SetFastAck()
{
#ifndef __APPLE__
    int quick_ack = 1;
    setsockopt(m_socket, IPPROTO_TCP, TCP_QUICKACK, (void *)&quick_ack, sizeof(quick_ack));
#endif
}

void BaseSocket::SetNonblock(int fd, bool nonblock)
{
    int ret = 0;
    int flags = fcntl(fd, F_GETFL);
    if (nonblock) {
        ret = fcntl(fd, F_SETFL, O_NONBLOCK | flags);
    } else {
        ret = fcntl(fd, F_SETFL, ~O_NONBLOCK & flags);
    }

	if (ret == SOCKET_ERROR) {
		printf("SetNonblock failed, err_code=%d\n", errno);
	}
}

void BaseSocket::SetReuseAddr(int fd)
{
	int reuse = 1;
	int ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse));
	if (ret == SOCKET_ERROR) {
		printf("SetReuseAddr failed, err_code=%d\n", errno);
	}
}

void BaseSocket::SetNoDelay(int fd)
{
	int nodelay = 1;
	int ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&nodelay, sizeof(nodelay));
	if (ret == SOCKET_ERROR) {
		printf("SetNoDelay failed, err_code=%d\n", errno);
	}
}

void BaseSocket::SetAddr(const char* ip, const uint16_t port, sockaddr_in* pAddr)
{
	memset(pAddr, 0, sizeof(sockaddr_in));
	pAddr->sin_family = AF_INET;
	pAddr->sin_port = htons(port);
	pAddr->sin_addr.s_addr = inet_addr(ip);
	if (pAddr->sin_addr.s_addr == INADDR_NONE) {
		hostent* host = gethostbyname(ip);
		if (host == NULL) {
			printf("gethostbyname failed, ip=%s\n", ip);
			return;
		}

		pAddr->sin_addr.s_addr = *(uint32_t*)host->h_addr;
	}
}

void BaseSocket::GetBindAddr(int fd, string& bind_ip, uint16_t& bind_port)
{
    struct sockaddr_in local_addr;
    socklen_t len = sizeof(local_addr);
    getsockname(fd, (sockaddr*)&local_addr, &len);
    uint32_t ip = ntohl(local_addr.sin_addr.s_addr);
    char ip_str[64];
    snprintf(ip_str, sizeof(ip_str), "%d.%d.%d.%d", ip >> 24, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
    bind_ip = ip_str;
    bind_port = ntohs(local_addr.sin_port);
}

bool BaseSocket::_IsBlock(int error_code)
{
	return ( (error_code == EINPROGRESS) || (error_code == EWOULDBLOCK) );
}

void BaseSocket::_AcceptNewSocket()
{
	int fd = 0;
	sockaddr_in peer_addr;
	socklen_t addr_len = sizeof(sockaddr_in);
	char ip_str[64];

	while (true) {
        fd = accept(m_socket, (sockaddr*)&peer_addr, &addr_len);
        if (fd == INVALID_SOCKET) {
            if (errno != EWOULDBLOCK) {
                printf("accept errno=%d\n", errno);
            }
            
            break;
        }
        
		BaseSocket* pSocket = new BaseSocket();

		uint32_t ip = ntohl(peer_addr.sin_addr.s_addr);
		uint16_t port = ntohs(peer_addr.sin_port);

		snprintf(ip_str, sizeof(ip_str), "%d.%d.%d.%d", ip >> 24, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);

		pSocket->SetSocket(fd);
		pSocket->SetCallback(m_callback);
		pSocket->SetCallbackData(m_callback_data);
		pSocket->SetState(SOCKET_STATE_CONNECTED);
		pSocket->SetRemoteIP(ip_str);
		pSocket->SetRemotePort(port);

		SetNoDelay(fd);
		SetNonblock(fd, true);
        
        EventLoop* client_event_loop = get_io_event_loop(pSocket->GetHandle());
        pSocket->SetEventLoop(client_event_loop);
		client_event_loop->AddEvent(fd, SOCKET_READ | SOCKET_EXCEP | SOCKET_CONNECT_CB| SOCKET_ADD_CONN, pSocket);
	}
}

