//
//  slowlog.h
//  kedis
//
//  Created by ziteng on 17/11/2.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __SLOWLOG_H__
#define __SLOWLOG_H__

#include "server.h"

#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

typedef struct {
    vector<string> cmd_vec;
    uint64_t slowlog_id;  // Unique entry identifier
    uint64_t duration; // Time spent by the query, in milliseconds
    time_t time; // Unix time at which the query was executed
} SlowlogEntry;

void add_slowlog(const vector<string>& cmd_vec, uint64_t duration);

void slowlog_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __SLOWLOG_H__ */
