//
//  redis_conn.cpp
//  kv-store
//
//  Created by ziteng on 17-11-16.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#include "redis_conn.h"
#include "simple_log.h"

RedisConn::RedisConn(const string& ip, int port) : ip_(ip), port_(port)
{
    context_ = NULL;
    reply_ = NULL;
}

RedisConn::RedisConn()
{
    context_ = NULL;
    reply_ = NULL;
}

RedisConn::~RedisConn()
{
    if (context_) {
        redisFree(context_);
        context_ = NULL;
    }
    
    if (reply_) {
        freeReplyObject(reply_);
        reply_ = NULL;
    }
}

int RedisConn::Init()
{
    if (context_) {
        return 0;
    }
    
    // 200ms timeout
    struct timeval timeout = {0, 200000};
    context_ = redisConnectWithTimeout(ip_.c_str(), port_, timeout);
    if (!context_ || context_->err) {
        if (context_) {
            log_message(kLogLevelError, "redisConnect failed: %s\n", context_->errstr);
            redisFree(context_);
            context_ = NULL;
        } else {
            log_message(kLogLevelError, "redisConnect failed\n");
        }
        
        return 1;
    }
    
    if (!password_.empty()) {
        string auth_cmd = "AUTH " + password_;
        reply_ = (redisReply *)redisCommand(context_, auth_cmd.c_str());
        if (reply_ && (reply_->type == REDIS_REPLY_STATUS) && !strncmp(reply_->str, "OK", 2)) {
            return 0;
        } else if (reply_ && (reply_->type == REDIS_REPLY_ERROR) && !strncmp(reply_->str, "ERR Client sent AUTH", 20)) {
            return 0;
        } else {
            log_message(kLogLevelError, "redis auth failed\n");
            return 2;
        }
    }
    
    return 0;
}

redisReply* RedisConn::DoRawCmd(const string& cmd)
{
    if (reply_) {
        freeReplyObject(reply_);
        reply_ = NULL;
    }
    
    if (Init()) {
        return NULL;
    }
    
    reply_ = (redisReply *)redisRawCommand(context_, cmd.c_str() , (int)cmd.size());
    if (!reply_) {
        log_message(kLogLevelError, "redisRawCommand failed: %s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return NULL;
    }
    
    return reply_;
}

redisReply* RedisConn::DoCmd(const string& cmd)
{
    if (reply_) {
        freeReplyObject(reply_);
        reply_ = NULL;
    }
    
    if (Init()) {
        return NULL;
    }
    
    reply_ = (redisReply *)redisCommand(context_, cmd.c_str());
    if (!reply_) {
        log_message(kLogLevelError, "redisCommand failed: %s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return NULL;
    }
    
    return reply_;
}

void RedisConn::PipelineRawCmd(const string& cmd)
{
    if (reply_) {
        freeReplyObject(reply_);
        reply_ = NULL;
    }
    
    if (Init()) {
        return;
    }
    
    redisAppendFormattedCommand(context_, cmd.c_str(), cmd.size());
}

void RedisConn::PipelineCmd(const string& cmd)
{
    if (reply_) {
        freeReplyObject(reply_);
        reply_ = NULL;
    }
    
    if (Init()) {
        return;
    }
    
    redisAppendCommand(context_, cmd.c_str());
}

redisReply* RedisConn::GetReply()
{
    if (reply_) {
        freeReplyObject(reply_);
        reply_ = NULL;
    }
    
    redisGetReply(context_, (void **)&reply_);
    
    return reply_;
}
