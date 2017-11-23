//
//  redis_conn.h
//  kv-store
//
//  Created by ziteng on 17-11-16.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#ifndef __REDIS_CONN_H__
#define __REDIS_CONN_H__

#include "util.h"
#include "hiredis.h"

class RedisConn
{
public:
    RedisConn(const string& ip, int port);
    RedisConn();
    virtual ~RedisConn();
    
    void SetAddr(const string& ip, int port) { ip_ = ip; port_ = port; }
    void SetPassword(const string& password) { password_ = password; }
    int Init();
    
    // Caution: do not freeReplyObject(), reply will be freed by the RedisConn object
    redisReply* DoRawCmd(const string& cmd);
    redisReply* DoCmd(const string& cmd);
    
    void PipelineRawCmd(const string& cmd);
    void PipelineCmd(const string& cmd);
    redisReply* GetReply();
private:
    redisContext*   context_;
    redisReply*     reply_; //reply_ will be freed in the next request
    string          ip_;
    int             port_;
    string          password_;
};

#endif
