/*
 * base_conn.cpp
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#include "base_conn.h"
#include "event_loop.h"
#include "io_thread_resource.h"
#include "simple_log.h"

typedef list<pair<net_handle_t, SimpleBuffer*>> PendingDataList;
typedef list<net_handle_t>  PendingCloseList;
typedef unordered_map<net_handle_t, BaseConn*> ConnMap_t;

atomic<long> BaseConn::m_total_net_input_bytes {0};
atomic<long> BaseConn::m_total_net_output_bytes {0};

struct PendingEventMgr {
    ConnMap_t           conn_map;
    mutex               mtx;
    PendingDataList     data_list;
    PendingCloseList    close_list;
};

IoThreadResource<PendingEventMgr> g_pending_event_mgr;

void loop_callback(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
    PendingEventMgr* event_mgr = (PendingEventMgr*)callback_data;
    if (!event_mgr)
        return;

    PendingDataList tmp_data_list;
    PendingCloseList tmp_close_list;
    {
        lock_guard<mutex> mg(event_mgr->mtx);
        tmp_data_list.swap(event_mgr->data_list);
        tmp_close_list.swap(event_mgr->close_list);
    }
    
    for (auto it = tmp_data_list.begin(); it != tmp_data_list.end(); ++it) {
        net_handle_t handle = it->first;
        SimpleBuffer* buf = it->second;
        ConnMap_t::iterator it_conn = event_mgr->conn_map.find(handle);
        if (it_conn != event_mgr->conn_map.end()) {
            BaseConn* conn = it_conn->second;
            if (conn->IsOpen()) {
                conn->Send(buf->GetReadBuffer(), buf->GetReadableLen());
            }
        }
        
        delete buf;
    }
    
    for (auto it = tmp_close_list.begin(); it != tmp_close_list.end(); ++it) {
        net_handle_t handle = *it;
        ConnMap_t::iterator it_conn = event_mgr->conn_map.find(handle);
        if (it_conn != event_mgr->conn_map.end()) {
            it_conn->second->Close();
        }
    }
    
    for (auto it = event_mgr->conn_map.begin(); it != event_mgr->conn_map.end(); it++) {
        it->second->OnLoop();
    }
}

void conn_callback(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
    //printf("conn_callback, msg=%d, handle=%d\n", msg, handle);
    
    BaseConn* pConn = (BaseConn*)callback_data;
	if (!pConn)
		return;

    pConn->AddRef();
	switch (msg) {
        case NETLIB_MSG_CONFIRM:
            pConn->OnConfirm();
            break;
        case NETLIB_MSG_READ:
            pConn->OnRead();
            break;
        case NETLIB_MSG_WRITE:
            pConn->OnWrite();
            break;
        case NETLIB_MSG_CLOSE:
            pConn->OnClose();
            break;
        case NETLIB_MSG_TIMER:
            {
                uint64_t curr_tick = *(uint64_t*)pParam;
                pConn->OnTimer(curr_tick);
                break;
            }
        default:
            printf("!!!conn_callback error msg: %d\n", msg);
            break;
	}
    pConn->ReleaseRef();
}

void init_thread_base_conn(int io_thread_num)
{
    if (g_pending_event_mgr.IsInited())
        return;
    
    g_pending_event_mgr.Init(io_thread_num);
    
    if (io_thread_num > 0) {
        for (int i = 0; i < io_thread_num; ++i) {
            EventLoop* el = get_io_event_loop(i);
            el->AddLoop(loop_callback, g_pending_event_mgr.GetIOResource(i));
        }
    } else {
        get_main_event_loop()->AddLoop(loop_callback, g_pending_event_mgr.GetMainResource());
    }
}

void destroy_thread_base_conn(int io_thread_num)
{
    for (int i = 0; i < io_thread_num; i++) {
        PendingEventMgr* event_mgr = g_pending_event_mgr.GetIOResource(i);
        for (auto it = event_mgr->data_list.begin(); it != event_mgr->data_list.end(); ++it) {
            SimpleBuffer* buf = it->second;
            delete buf;
        }
    }
}

//////////////////////////
BaseConn::BaseConn()
{
    m_thread_index = -1;
    m_base_socket = NULL;
    m_open = false;
	m_busy = false;
	m_handle = NETLIB_INVALID_HANDLE;
    m_heartbeat_interval = kHeartBeartInterval;
    m_conn_timeout = kConnTimeout;
    
	m_last_send_tick = m_last_recv_tick = get_tick_count();
}

BaseConn::~BaseConn()
{
    for (auto it = m_out_list.begin(); it != m_out_list.end(); ++it) {
        if ((it->type == SENDING_OBJ_TYPE_FILE) && it->file.fp) {
            fclose(it->file.fp);
        }
    }
    
    m_out_list.clear();
}

net_handle_t BaseConn::Connect(const string& server_ip, uint16_t server_port, int thread_index)
{
    m_peer_ip = server_ip;
    m_peer_port = server_port;
    
    EventLoop* event_loop = NULL;
    if (thread_index != -1) {
        event_loop = get_io_event_loop(thread_index);
    }
    
    m_base_socket = new BaseSocket();
    assert(m_base_socket);
    m_handle = m_base_socket->Connect(server_ip.c_str(), server_port, conn_callback, this, event_loop);
    if (thread_index != -1) {
        m_thread_index = thread_index;
    } else {
        m_thread_index = m_handle;
    }
    
    if (m_handle == NETLIB_INVALID_HANDLE) {
        delete m_base_socket;
        delete this;
    } else {
        if (g_pending_event_mgr.IsInited()) {
            PendingEventMgr* event_mgr = g_pending_event_mgr.GetIOResource(m_thread_index);
            event_mgr->conn_map.insert(make_pair(m_handle, this));
        }
    }
    
    return m_handle;
}

void BaseConn::Close()
{
    if (m_handle == NETLIB_INVALID_HANDLE) {
        return;
    }
    
    if (g_pending_event_mgr.IsInited()) {
        PendingEventMgr* event_mgr = g_pending_event_mgr.GetIOResource(m_thread_index);
        event_mgr->conn_map.erase(m_handle);
    }
    
    m_base_socket->Close();
    m_open = false;
    m_busy = false;
    m_in_buf.Clear();
    m_out_buf.Clear();
    m_handle = NETLIB_INVALID_HANDLE;
    ReleaseRef();
}

int BaseConn::Send(void* data, int len)
{
	m_last_send_tick = get_tick_count();

    if (!m_open) {
        printf("connection do not open yet\n");
        return 0;
    }
    
	if (m_busy) {
        if (m_out_list.empty()) {
            m_out_buf.Write(data, len);
        } else {
            SendingObject obj = m_out_list.back();
            if (obj.type == SENDING_OBJ_TYPE_BUF) {
                obj.buf.Write(data, len);
            } else {
                SendingObject new_obj;
                new_obj.type = SENDING_OBJ_TYPE_BUF;
                new_obj.buf.Write(data, len);
                m_out_list.push_back(new_obj);
            }
        }
		return len;
	}

	int offset = 0;
	int remain = len;
	while (remain > 0) {
		int send_size = remain;
		if (send_size > kMaxSendSize) {
			send_size = kMaxSendSize;
		}

		int ret = m_base_socket->Send((char*)data + offset , send_size);
		if (ret <= 0) {
			ret = 0;
			break;
		}

        m_total_net_output_bytes += ret;
		offset += ret;
		remain -= ret;
	}

	if (remain > 0) {
		m_out_buf.Write((char*)data + offset, remain);
		m_busy = true;
	}

	return len;
}

int BaseConn::SendFile(const string& filename)
{
    if (!m_open) {
        printf("connection do not open yet\n");
        return -1;
    }
    
    struct stat st;
    if (stat(filename.c_str(), &st) == -1) {
        printf("stat failed\n");
        return -1;
    }

    struct SendingObject obj;
    obj.type = SENDING_OBJ_TYPE_FILE;
    obj.file.filename = filename;
    obj.file.offset = 0;
    obj.file.size = st.st_size;
    obj.file.fp = fopen(filename.c_str(), "rb");
    if (!obj.file.fp) {
        printf("open file failed\n");
        return -1;
    }
    
    m_out_list.push_back(obj);
    
    if (m_busy) {
        return 0;
    }
    
    // if not busy, there is only one SendingObject
    SendingObject obj_ref = m_out_list.front();
    _SendFile(obj_ref.file);
    if (obj_ref.file.offset == obj_ref.file.size) {
        m_out_list.pop_front();
    } else {
        m_busy = true;
    }
    
    return 0;
}

void BaseConn::OnConnect(BaseSocket *base_socket)
{
    m_open = true;
    m_base_socket = base_socket;
    m_handle = base_socket->GetHandle();
    m_thread_index = m_handle;
    
    if (g_pending_event_mgr.IsInited()) {
        PendingEventMgr* event_mgr = g_pending_event_mgr.GetIOResource(m_thread_index);
        event_mgr->conn_map.insert(make_pair(m_handle, this));
    }
    
    base_socket->SetCallback(conn_callback);
    base_socket->SetCallbackData((void*)this);
    m_peer_ip = base_socket->GetRemoteIP();
    m_peer_port = base_socket->GetRemotePort();
}

void BaseConn::OnConfirm()
{
    m_open = true;
    m_busy = false;
}

void BaseConn::OnRead()
{
    
}

void BaseConn::OnWrite()
{
	if (!m_busy)
		return;

    _SendBuffer(m_out_buf);
	if (m_out_buf.GetReadableLen() != 0) {
        return;
	}

    while (!m_out_list.empty()) {
        SendingObject obj = m_out_list.front();
        if (obj.type == SENDING_OBJ_TYPE_BUF) {
            _SendBuffer(obj.buf);
            if (obj.buf.GetReadableLen() == 0) {
                m_out_list.pop_front();
            } else {
                break;
            }
        } else {
            _SendFile(obj.file);
            if (obj.file.offset == obj.file.size) {
                m_out_list.pop_front();
            } else {
                break;
            }
        }
    }
    
    if (m_out_list.empty()) {
        m_busy = false;
    }
}

void BaseConn::OnClose()
{
    Close();
}

void BaseConn::OnTimer(uint64_t curr_tick)
{
    //printf("OnTimer, curr_tick=%llu\n",curr_tick);
    if (curr_tick > m_last_recv_tick + m_conn_timeout) {
        printf("connection timeout, handle=%d\n", m_handle);
        Close();
        return;
    }
}

void BaseConn::_RecvData()
{
    for (;;) {
		uint32_t free_buf_len = m_in_buf.GetWritableLen();
		if (free_buf_len < (kReadBufSize + 1))  // reserve 1 byte for text protocol to add '\0'
			m_in_buf.Extend(kReadBufSize + 1);
        
		int ret = m_base_socket->Recv(m_in_buf.GetWriteBuffer(), kReadBufSize);
		if (ret <= 0)
			break;
        
        m_total_net_input_bytes += ret;
		m_in_buf.IncWriteOffset(ret);
		m_last_recv_tick = get_tick_count();
	}
}

void BaseConn::_SendBuffer(SimpleBuffer& buf)
{
    while (buf.GetReadableLen() > 0) {
        int send_size = buf.GetReadableLen();
        if (send_size > kMaxSendSize) {
            send_size = kMaxSendSize;
        }
        
        int ret = m_base_socket->Send(buf.GetReadBuffer(), send_size);
        if (ret <= 0) {
            ret = 0;
            break;
        }
        
        m_total_net_output_bytes += ret;
        buf.Read(NULL, ret);
    }
    
    buf.ResetOffset();
}

void BaseConn::_SendFile(SendingFile& file)
{
    char buf[kReadBufSize];
    while (true) {
        fseek(file.fp, file.offset, SEEK_SET);
        int nread = (int)fread(buf, 1, kReadBufSize, file.fp);
        if (nread <= 0) {
            Close();
            break;
        }
        
        int nwritten = m_base_socket->Send(buf, nread);
        if (nwritten <= 0) {
            break;
        }
        
        m_total_net_output_bytes += nwritten;
        file.offset += nwritten;
        if (file.offset == file.size) {
            fclose(file.fp);
            file.fp = NULL;
            break;
        }
    }
}

// static methods

// used for master to synchronize with slave
int BaseConn::Send(net_handle_t handle, void* data, int len)
{
    if (handle == NETLIB_INVALID_HANDLE) {
        return 0;
    }
    
    EventLoop* el = get_io_event_loop(handle);
    PendingEventMgr* event_mgr = g_pending_event_mgr.GetIOResource(handle);

    // even handle is in the same IO thread, the data still need push to the end of the list,
    // so the sequence will same between master and slave
    SimpleBuffer* buf = new SimpleBuffer();
    buf->Write(data, len);
    event_mgr->mtx.lock();
    event_mgr->data_list.push_back(make_pair(handle, buf));
    event_mgr->mtx.unlock();
    
    el->Wakeup();
    
    return 0;
}

int BaseConn::CloseHandle(net_handle_t handle)
{
    if (handle == NETLIB_INVALID_HANDLE) {
        return 0;
    }
    
    EventLoop* el = get_io_event_loop(handle);
    PendingEventMgr* event_mgr = g_pending_event_mgr.GetIOResource(handle);
    
    if (el->IsInLoopThread()) {
        ConnMap_t::iterator it_conn = event_mgr->conn_map.find(handle);
        if (it_conn != event_mgr->conn_map.end()) {
            it_conn->second->Close();
        }
    } else {
        event_mgr->mtx.lock();
        event_mgr->close_list.push_back(handle);
        event_mgr->mtx.unlock();
        
        el->Wakeup();
    }
    
    return 0;
}

