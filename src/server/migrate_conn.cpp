//
//  migrate_conn.cpp
//  kedis
//
//  Created by ziteng on 17/9/19.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "migrate_conn.h"
#include "migrate.h"
#include "simple_log.h"

MigrateConn::MigrateConn()
{
    
}

MigrateConn::~MigrateConn()
{
    
}

void MigrateConn::Close()
{
    BaseConn::Close();
    migrate_conn_closed(addr_);
}

void MigrateConn::OnConfirm()
{
    BaseConn::OnConfirm();
    migrate_conn_established(addr_);
}

void MigrateConn::OnRead()
{
    _RecvData();
    
    // when parse redis protocol, this makes sure searching string pattern will not pass the boundary
    m_in_buf.GetWriteBuffer()[0] = 0;
    
    while (true) {
        RedisReply reply;
        int ret = parse_redis_response((const char*)m_in_buf.GetReadBuffer(), m_in_buf.GetReadableLen(), reply);
        if (ret > 0) {
            if (reply.GetType() == REDIS_TYPE_ERROR) {
                log_message(kLogLevelError, "migrate error: %s\n", reply.GetStrValue().c_str());
                return;
            }
            
            continue_migrate_command(addr_, reply);
            m_in_buf.Read(NULL, ret);
        } else if (ret < 0) {
            log_message(kLogLevelError, "parse redis protocol error in migrate connection\n");
            Close();
            return;
        } else {
            // not yet receive a whole command
            m_in_buf.ResetOffset();
            break;
        }
    }
}

void MigrateConn::OnTimer(uint64_t curr_tick)
{
    if (!IsOpen()) {
        if (curr_tick >= m_last_send_tick + 1000) {
            // if connection has not establish after 1 second, close the connection
            Close();
        }
    } else {
        if (curr_tick >= m_last_send_tick + 15000) {
            // if connection idled 15 seconds, close the connection
            Close();
        }
    }
}
