//
//  cmd_db.h
//  kedis
//
//  Created by ziteng on 17/7/27.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CMD_DB_H__
#define __CMD_DB_H__

#include "server.h"

void auth_command(ClientConn* conn, const vector<string>& cmd_vec);
void ping_command(ClientConn* conn, const vector<string>& cmd_vec);
void echo_command(ClientConn* conn, const vector<string>& cmd_vec);
void select_command(ClientConn* conn, const vector<string>& cmd_vec);
void dbsize_command(ClientConn* conn, const vector<string>& cmd_vec);
void info_command(ClientConn* conn, const vector<string>& cmd_vec);

void generic_flushdb(int db_idx);
void flushdb_command(ClientConn* conn, const vector<string>& cmd_vec);
void flushall_command(ClientConn* conn, const vector<string>& cmd_vec);
void debug_command(ClientConn* conn, const vector<string>& cmd_vec);
void command_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CMD_DB_H__ */
