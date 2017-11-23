//
//  migrate.h
//  kedis
//
//  Created by ziteng on 17/9/18.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __MIGRATE_H__
#define __MIGRATE_H__

#include "server.h"

int get_migrate_conn_number();

int dump_string(const string& key, uint64_t ttl, const string& encode_value, string& serialized_value);
int dump_complex_struct(int db_idx, uint8_t type, const string& key, uint64_t ttl, const string& encode_value,
                        uint64_t count, string& serialized_value, const rocksdb::Snapshot* snapshot = NULL);

void migrate_conn_established(const string& addr);
void migrate_conn_closed(const string& addr);
void continue_migrate_command(const string& addr, const RedisReply& reply);

void migrate_command(ClientConn* conn, const vector<string>& cmd_vec);
void restore_command(ClientConn* conn, const vector<string>& cmd_vec);
void dump_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __MIGRATE_H__ */
