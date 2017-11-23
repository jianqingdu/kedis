//
//  cmd_list.h
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CMD_LIST_H__
#define __CMD_LIST_H__

#include "server.h"

void lindex_command(ClientConn* conn, const vector<string>& cmd_vec);
void linsert_command(ClientConn* conn, const vector<string>& cmd_vec);
void llen_command(ClientConn* conn, const vector<string>& cmd_vec);
void lpush_command(ClientConn* conn, const vector<string>& cmd_vec);
void lpop_command(ClientConn* conn, const vector<string>& cmd_vec);
void lrange_command(ClientConn* conn, const vector<string>& cmd_vec);
void rpush_command(ClientConn* conn, const vector<string>& cmd_vec);
void rpop_command(ClientConn* conn, const vector<string>& cmd_vec);

void lpushx_command(ClientConn* conn, const vector<string>& cmd_vec);
void rpushx_command(ClientConn* conn, const vector<string>& cmd_vec);
void lrem_command(ClientConn* conn, const vector<string>& cmd_vec);
void lset_command(ClientConn* conn, const vector<string>& cmd_vec);
void ltrim_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CMD_LIST_H__ */
