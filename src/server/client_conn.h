//
//  client_conn.h
//  kedis
//
//  Created by ziteng on 17/7/19.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CLIENT_CONN_H__
#define __CLIENT_CONN_H__

#include "base_conn.h"
#include "redis_parser.h"

enum {
    CONN_STATE_IDLE = 0,
    CONN_STATE_RECV_AUTH,
    CONN_STATE_RECV_PORT,
    CONN_STATE_RECV_PSYNC,
    CONN_STATE_RECV_SNAPSHOT,
    CONN_STATE_CONNECTED,
};

enum {
    CLIENT_NORMAL = 0, // normal client
    CLIENT_SLAVE, // the client is a slave
    CLIENT_MASTER, // the client is a master
};

class ReplicationSnapshot;

class ClientConn : public BaseConn {
public:
    ClientConn();
    virtual ~ClientConn();
    
    virtual void Close();
    virtual void OnConnect(BaseSocket* base_socket);
    virtual void OnConfirm();
    virtual void OnRead();
    virtual void OnTimer(uint64_t curr_tick);
    virtual void OnLoop();
    
    void SendRawResponse(const string& resp);
    void SendError(const string& error_msg);
    void SendInteger(long i);
    void SendSimpleString(const string& str);
    void SendBulkString(const string& str);
    void SendArray(const vector<string>& str_vec);
    void SendMultiBuldLen(long len);
    
    int GetDBIndex() { return db_index_; }
    void SetAuth(bool auth) { authenticated_ = auth; }
    string GetCurReqCommand() { return string(cur_req_buf_, cur_req_len_); }
    void SetState(int state) { state_ = state; }
    int GetState() { return state_; }
    void SetFlag(int flag) { flag_ = flag; }
    void SetSlavePort(int port) { slave_port_ = port; }
    void SetSyncSeq(uint64_t seq) { sync_seq_ = seq; }
    void SetDbIndex(int db_idx) { db_index_ = db_idx; }
    void SetReplSnapshot(ReplicationSnapshot* snapshot) { repl_snapshot_ = snapshot; }
    bool IsSendingSnapshot() { return repl_snapshot_ != nullptr; }
    uint64_t GetSyncSeq() { return sync_seq_; };
    int GetSlavePort() { return slave_port_; }
    string GetSlaveName() { return m_peer_ip + ":" + to_string(slave_port_); }
private:
    void _HandleRedisCommand(vector<string>& cmd_vec);
private:
    int     db_index_;
    string  pipeline_response_;
    bool    authenticated_;
    char*   cur_req_buf_; // the buffer of the current processing request
    int     cur_req_len_; // the length of current processing request
    int     state_;
    int     flag_;  // client type
    int     slave_port_;
    uint64_t sync_seq_;  // used for slave conn, the binlog sequence that need to send to slave
    ReplicationSnapshot* repl_snapshot_;
};

#endif
