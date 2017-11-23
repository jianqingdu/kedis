//
//  slowlog.cpp
//  kedis
//
//  Created by ziteng on 17/11/2.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "slowlog.h"
#include "db_util.h"

mutex g_slowlog_mtx;
list<SlowlogEntry*> g_slowlog_list;
uint64_t g_slowlog_id = 0;

void add_slowlog(const vector<string>& cmd_vec, uint64_t duration)
{
    int cmd_size = (int)cmd_vec.size();
    int sl_argc = cmd_size;
    if (sl_argc > SLOWLOG_ENTRY_MAX_ARGC) {
        sl_argc = SLOWLOG_ENTRY_MAX_ARGC;
    }
    
    SlowlogEntry* sl_entry = new SlowlogEntry;
    
    lock_guard<mutex> guard(g_slowlog_mtx);
    if ((int)g_slowlog_list.size() >= g_server.slowlog_max_len) {
        SlowlogEntry* old_entry = g_slowlog_list.front();
        delete old_entry;
        g_slowlog_list.pop_front();
    }
    
    for (int i = 0; i < sl_argc; i++) {
        if ((sl_argc != cmd_size) && (i == sl_argc - 1)) {
            char buf[256];
            snprintf(buf, sizeof(buf), "... (%d more arguments)", cmd_size - sl_argc + 1);
            sl_entry->cmd_vec.push_back(buf);
        } else {
            // Trim too long strings
            int item_size = (int)cmd_vec[i].size();
            if (item_size > SLOWLOG_ENTRY_MAX_STRING) {
                char buf[256];
                string prefix = cmd_vec[i].substr(0, SLOWLOG_ENTRY_MAX_STRING);
                snprintf(buf, sizeof(buf), "%s... (%d more bytes)",
                         prefix.c_str(),  item_size - SLOWLOG_ENTRY_MAX_STRING);
                sl_entry->cmd_vec.push_back(buf);
            } else {
                sl_entry->cmd_vec.push_back(cmd_vec[i]);
            }
        }
    }
    
    sl_entry->time = time(NULL);
    sl_entry->duration = duration;
    sl_entry->slowlog_id = g_slowlog_id++;
    g_slowlog_list.push_back(sl_entry);
}

void slowlog_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (!strcasecmp(cmd_vec[1].c_str(), "reset") && (cmd_size == 2)) {
        g_slowlog_mtx.lock();
        for (SlowlogEntry* entry : g_slowlog_list) {
            delete entry;
        }
        g_slowlog_list.clear();
        g_slowlog_mtx.unlock();
        
        conn->SendRawResponse(kOKString);
    } else if (!strcasecmp(cmd_vec[1].c_str(), "len") && (cmd_size == 2)) {
        g_slowlog_mtx.lock();
        long len = (long)g_slowlog_list.size();
        g_slowlog_mtx.unlock();
        
        conn->SendInteger(len);
    } else if (!strcasecmp(cmd_vec[1].c_str(), "get") && (cmd_size == 2 || cmd_size == 3)) {
        long count = 10;
        if (cmd_size == 3) {
            if (get_long_from_string(cmd_vec[2], count) == CODE_ERROR) {
                conn->SendError("value is not integer or out of range");
                return;
            }
        }
        
        lock_guard<mutex> guard(g_slowlog_mtx);
        long sl_size = (long)g_slowlog_list.size();
        if (count > sl_size) {
            count = sl_size;
        }
        
        conn->SendMultiBuldLen(count);
        for (auto it = g_slowlog_list.rbegin(); it != g_slowlog_list.rend() && count; it++) {
            SlowlogEntry* entry = *it;
            conn->SendMultiBuldLen(4);
            conn->SendInteger(entry->slowlog_id);
            conn->SendInteger(entry->time);
            conn->SendInteger(entry->duration);
            conn->SendArray(entry->cmd_vec);
            count--;
        }
    } else {
        conn->SendError("Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN");
    }
}
