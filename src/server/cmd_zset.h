//
//  cmd_zset.h
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CMD_ZSET_H__
#define __CMD_ZSET_H__

#include "server.h"

void zadd_command(ClientConn* conn, const vector<string>& cmd_vec);
void zincrby_command(ClientConn* conn, const vector<string>& cmd_vec);
void zcard_command(ClientConn* conn, const vector<string>& cmd_vec);
void zcount_command(ClientConn* conn, const vector<string>& cmd_vec);

void zrange_command(ClientConn* conn, const vector<string>& cmd_vec);
void zrevrange_command(ClientConn* conn, const vector<string>& cmd_vec);
void zrangebyscore_command(ClientConn* conn, const vector<string>& cmd_vec);
void zrevrangebyscore_command(ClientConn* conn, const vector<string>& cmd_vec);
void zlexcount_command(ClientConn* conn, const vector<string>& cmd_vec);
void zrangebylex_command(ClientConn* conn, const vector<string>& cmd_vec);
void zrevrangebylex_command(ClientConn* conn, const vector<string>& cmd_vec);

void zrank_command(ClientConn* conn, const vector<string>& cmd_vec);
void zrevrank_command(ClientConn* conn, const vector<string>& cmd_vec);

void zrem_command(ClientConn* conn, const vector<string>& cmd_vec);
void zremrangebylex_command(ClientConn* conn, const vector<string>& cmd_vec);
void zremrangebyrank_command(ClientConn* conn, const vector<string>& cmd_vec);
void zremrangebyscore_command(ClientConn* conn, const vector<string>& cmd_vec);
void zscore_command(ClientConn* conn, const vector<string>& cmd_vec);
void zscan_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CMD_ZSET_H__ */
