//
//  cmd_set.h
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CMD_SET_H__
#define __CMD_SET_H__

#include "server.h"

void sadd_command(ClientConn* conn, const vector<string>& cmd_vec);
void scard_command(ClientConn* conn, const vector<string>& cmd_vec);
void sismember_command(ClientConn* conn, const vector<string>& cmd_vec);
void smembers_command(ClientConn* conn, const vector<string>& cmd_vec);
void spop_command(ClientConn* conn, const vector<string>& cmd_vec);
void srandmember_command(ClientConn* conn, const vector<string>& cmd_vec);
void srem_command(ClientConn* conn, const vector<string>& cmd_vec);
void sscan_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CMD_SET_H__ */
