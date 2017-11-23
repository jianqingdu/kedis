//
//  cmd_string.h
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CMD_STRING_H__
#define __CMD_STRING_H__

#include "server.h"

void append_command(ClientConn* conn, const vector<string>& cmd_vec);
void getrange_command(ClientConn* conn, const vector<string>& cmd_vec);
void setrange_command(ClientConn* conn, const vector<string>& cmd_vec);
void strlen_command(ClientConn* conn, const vector<string>& cmd_vec);

void get_command(ClientConn* conn, const vector<string>& cmd_vec);
void getset_command(ClientConn* conn, const vector<string>& cmd_vec);
void set_command(ClientConn* conn, const vector<string>& cmd_vec);
void setex_command(ClientConn* conn, const vector<string>& cmd_vec);
void setnx_command(ClientConn* conn, const vector<string>& cmd_vec);
void psetex_command(ClientConn* conn, const vector<string>& cmd_vec);

void mset_command(ClientConn* conn, const vector<string>& cmd_vec);
void msetnx_command(ClientConn* conn, const vector<string>& cmd_vec);
void mget_command(ClientConn* conn, const vector<string>& cmd_vec);

void incr_command(ClientConn* conn, const vector<string>& cmd_vec);
void decr_command(ClientConn* conn, const vector<string>& cmd_vec);
void incrby_command(ClientConn* conn, const vector<string>& cmd_vec);
void decrby_command(ClientConn* conn, const vector<string>& cmd_vec);
void incrbyfloat_command(ClientConn* conn, const vector<string>& cmd_vec);

void setbit_command(ClientConn* conn, const vector<string>& cmd_vec);
void getbit_command(ClientConn* conn, const vector<string>& cmd_vec);
void bitcount_command(ClientConn* conn, const vector<string>& cmd_vec);
void bitpos_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CMD_STRING_H__ */
