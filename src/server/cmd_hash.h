//
//  cmd_hash.h
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CMD_HASH_H__
#define __CMD_HASH_H__

#include "server.h"

void hdel_command(ClientConn* conn, const vector<string>& cmd_vec);
void hexists_command(ClientConn* conn, const vector<string>& cmd_vec);
void hget_command(ClientConn* conn, const vector<string>& cmd_vec);
void hgetall_command(ClientConn* conn, const vector<string>& cmd_vec);
void hkeys_command(ClientConn* conn, const vector<string>& cmd_vec);
void hvals_command(ClientConn* conn, const vector<string>& cmd_vec);

void hincrby_command(ClientConn* conn, const vector<string>& cmd_vec);
void hincrbyfloat_command(ClientConn* conn, const vector<string>& cmd_vec);

void hlen_command(ClientConn* conn, const vector<string>& cmd_vec);
void hmget_command(ClientConn* conn, const vector<string>& cmd_vec);
void hmset_command(ClientConn* conn, const vector<string>& cmd_vec);
void hscan_command(ClientConn* conn, const vector<string>& cmd_vec);
void hset_command(ClientConn* conn, const vector<string>& cmd_vec);
void hsetnx_command(ClientConn* conn, const vector<string>& cmd_vec);
void hstrlen_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CMD_HASH_H__ */
