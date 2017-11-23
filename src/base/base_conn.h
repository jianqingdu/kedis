/*
 * base_conn.h
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#ifndef __BASE_BASE_CONN_H_
#define __BASE_BASE_CONN_H_

#include "util.h"
#include "simple_buffer.h"
#include "base_socket.h"

const int kHeartBeartInterval =	3000;
const int kConnTimeout = 16000;

const int kMaxSendSize = 128 * 1024;
const int kReadBufSize = 2048;

struct SendingFile {
    string filename;
    FILE* fp;
    uint64_t offset;
    uint64_t size;
    
    SendingFile() : fp(NULL), offset(0), size(0) {}
};

#define SENDING_OBJ_TYPE_BUF    1
#define SENDING_OBJ_TYPE_FILE   2

struct SendingObject {
    uint8_t type;
    SimpleBuffer buf;
    SendingFile file;
};

class BaseConn : public RefCount
{
public:
	BaseConn();
	virtual ~BaseConn();

    bool IsBusy() { return m_busy; }
    bool IsOpen() { return m_open; }
    void SetHeartbeatInterval(int interval) { m_heartbeat_interval = interval; }
    void SetConnTimerout(int timeout) { m_conn_timeout = timeout; }
    net_handle_t GetHandle() { return m_handle; }
    char* GetPeerIP() { return (char*)m_peer_ip.c_str(); }
    uint16_t GetPeerPort() { return m_peer_port; }
    
    virtual net_handle_t Connect(const string& server_ip, uint16_t server_port, int thread_index = -1);
    int Send(void* data, int len);
    int SendFile(const string& filename);
    virtual void Close();
    
	virtual void OnConnect(BaseSocket* base_socket);
	virtual void OnConfirm();
	virtual void OnRead();
	virtual void OnWrite();
	virtual void OnClose();
	virtual void OnTimer(uint64_t curr_tick);
    virtual void OnLoop() {} // be called everytime before waiting for event
  
    static int Send(net_handle_t handle, void* data, int len);
    static int CloseHandle(net_handle_t handle);  // used for other thread to close the connection
    
    static long GetTotalNetInputBytes() { return m_total_net_input_bytes; }
    static long GetTotalNetOutputBytes() { return m_total_net_output_bytes; }
protected:
    void _RecvData();
    void _SendBuffer(SimpleBuffer& buf);
    void _SendFile(SendingFile& file);
protected:
    int             m_thread_index;
    BaseSocket*     m_base_socket;
	net_handle_t	m_handle;
	bool			m_busy;
    bool            m_open;
    int             m_heartbeat_interval;
    int             m_conn_timeout;
    
	string			m_peer_ip;
	uint16_t		m_peer_port;
	SimpleBuffer	m_in_buf;
	SimpleBuffer	m_out_buf;
    list<SendingObject> m_out_list;

	uint64_t		m_last_send_tick;
	uint64_t		m_last_recv_tick;
    
    static atomic<long> m_total_net_input_bytes;
    static atomic<long> m_total_net_output_bytes;
};

void init_thread_base_conn(int io_thread_num);
void destroy_thread_base_conn(int io_thread_num);

#endif /* __BASE_CONN_H_ */
