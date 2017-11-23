//
//  src_kedis_conn.h
//  kedis
//
//  Created by ziteng on 17/11/16.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __SRC_KEDIS_CONN_H__
#define __SRC_KEDIS_CONN_H__

#include "base_conn.h"

enum {
    CONN_STATE_IDLE = 0,
    CONN_STATE_RECV_AUTH,
    CONN_STATE_RECV_PORT,
    CONN_STATE_RECV_PSYNC,
    CONN_STATE_RECV_SNAPSHOT,
    CONN_STATE_CONNECTED,
};

class SrcKedisConn : public BaseConn {
public:
    SrcKedisConn();
    virtual ~SrcKedisConn();
    
    virtual void Close();
    virtual void OnConfirm();
    virtual void OnRead();
    virtual void OnTimer(uint64_t curr_tick);
private:
    void _RemoveKeyPrefix(string& key);
    void _HandleRedisCommand(vector<string>& cmd_vec);
private:
    int     state_;
    int     cur_db_num_;
    char*   req_buf;
    int     req_len;
};

#endif /* __SRC_KEDIS_CONN_H__ */
