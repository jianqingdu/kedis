//
//  cmd_keys.h
//  kedis
//
//  Created by ziteng on 17/7/25.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CMD_KEYS_H__
#define __CMD_KEYS_H__

#include "server.h"

void del_command(ClientConn* conn, const vector<string>& cmd_vec);
void exists_command(ClientConn* conn, const vector<string>& cmd_vec);
void ttl_command(ClientConn* conn, const vector<string>& cmd_vec);
void pttl_command(ClientConn* conn, const vector<string>& cmd_vec);
void type_command(ClientConn* conn, const vector<string>& cmd_vec);

void expire_command(ClientConn* conn, const vector<string>& cmd_vec);
void expireat_command(ClientConn* conn, const vector<string>& cmd_vec);
void pexpire_command(ClientConn* conn, const vector<string>& cmd_vec);
void pexpireat_command(ClientConn* conn, const vector<string>& cmd_vec);
void persist_command(ClientConn* conn, const vector<string>& cmd_vec);
void randomkey_command(ClientConn* conn, const vector<string>& cmd_vec);
void keys_command(ClientConn* conn, const vector<string>& cmd_vec);
void scan_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CMD_KEYS_H__ */
