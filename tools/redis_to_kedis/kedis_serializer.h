//
//  kedis_serializer.h
//  kedis
//
//  Created by ziteng on 17/11/21.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __KEDIS_SERIALIZER_H__
#define __KEDIS_SERIALIZER_H__

#include "util.h"

string serialize_string(uint64_t ttl, const string& key, const string& value);
string serialize_list(uint64_t ttl, const string& key, const list<string>& value_list);
string serialize_hash(uint64_t ttl, const string& key, const map<string, string>& fv_map);
string serialize_set(uint64_t ttl, const string& key, const set<string>& member_set);
string serialize_zset(uint64_t ttl, const string& key, const map<string, double>& score_map);

string serialize_list_vec(uint64_t ttl, const string& key, const vector<string>& vec);
string serialize_hash_vec(uint64_t ttl, const string& key, const vector<string>& vec);
string serialize_zset_vec(uint64_t ttl, const string& key, const vector<string>& vec);

#endif /* __KEDIS_SERIALIZER_H__ */
