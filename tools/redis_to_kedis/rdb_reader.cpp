//
//  rdb_reader.cpp
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#include "rdb_reader.h"
#include "lzf.h"
#include "redis_parser.h"
#include "cmd_line_parser.h"
#include "sync_task.h"
#include "simple_log.h"
#include "kedis_serializer.h"

RdbReader::RdbReader()
{
    rdb_file_ = NULL;
    cur_db_num_ = 0;
}

RdbReader::~RdbReader()
{
    if (rdb_file_) {
        fclose(rdb_file_);
        rdb_file_ = NULL;
    }
}

int RdbReader::Open(const string& rdb_file_path)
{
    rdb_file_ = fopen(rdb_file_path.c_str(), "rb");
    if (!rdb_file_) {
        log_message(kLogLevelError, "fopen failed\n");
        return 1;
    }
    
    char buf[16];
    if (fread(buf, 1, 9, rdb_file_) != 9) {
        log_message(kLogLevelError, "fread return error\n");
        return 2;
    }
    
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        log_message(kLogLevelError, "Wrong signature trying to load DB from file\n");
        return 3;
    }
    
    int rdb_version = atoi(buf + 5);
    if ((rdb_version < 1) || (rdb_version > REDIS_RDB_VERSION)) {
        log_message(kLogLevelError, "Can't handle RDB format version %d\n", rdb_version);
        return 4;
    }
    
    return 0;
}

int RdbReader::RestoreDB(int src_db_num, RedisConn& redis_conn)
{
    int64_t cur_ms_time = (int64_t)get_tick_count();
    int pipeline_cmd_cnt = 0;
    while (true) {
        /* Read type. */
        int type = _LoadType();
        if (type == -1) {
            return -1;
        }
        
        int64_t expire_time = -1;  // expire time in milliseconds
        if (type == REDIS_RDB_OPCODE_EXPIRETIME) {
            if ((expire_time = _LoadTime()) == -1) {
                return -1;
            }
            /* We read the time so we need to read the object type again. */
            if ((type = _LoadType()) == -1) {
                return -1;
            }

            expire_time *= 1000;
        } else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
            if ((expire_time = _LoadMsTime()) == -1) {
                return -1;
            }
            /* We read the time so we need to read the object type again. */
            if ((type = _LoadType()) == -1) {
                return -1;
            }
        } else if (type == REDIS_RDB_OPCODE_EOF) {
            break; // End of RDB file
        } else if (type == REDIS_RDB_OPCODE_SELECTDB) {
            // SELECTDB: Select the specified database
            bool is_encoded = false;
            if ((cur_db_num_ = _LoadLen(is_encoded)) == (int)REDIS_RDB_LENERR) {
                log_message(kLogLevelError, "read db number failed\n");
                return -1;
            }
            
            continue; // Read type again
        } else if (type == REDIS_RDB_OPCODE_RESIZEDB) {
            bool is_encoded = false;
            uint32_t db_size = _LoadLen(is_encoded);
            uint32_t expire_size = _LoadLen(is_encoded);
            // discard resize, just log the message
            log_message(kLogLevelInfo, "resizedb db_size=%d, expire_size=%d\n", db_size, expire_size);
            continue; // Read type again
        } else if (type == REDIS_RDB_OPCODE_AUX) {
            string key = _LoadString();
            string value = _LoadString();
            log_message(kLogLevelInfo, "AUX RDB: %s=%s\n", key.c_str(), value.c_str());
            continue; // Read type again
        }
        
        // read key
        string key = _LoadString();
        
        // remove prefix
        if (!g_config.prefix.empty()) {
            size_t pos = key.find(g_config.prefix);
            if (pos != string::npos) {
                key = key.substr(pos + g_config.prefix.size());
            }
        }
        
        uint64_t ttl = (expire_time == -1) ? 0 : (uint64_t)expire_time;
        string val = _LoadSerializedValue(type, ttl, key);
        
        if ((src_db_num != -1) && (src_db_num != cur_db_num_)) {
            continue;
        }
        
        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the slave. */
        if ((expire_time != -1) && (expire_time <= cur_ms_time)) {
            continue;
        }
        
        string ttl_str = (expire_time == -1) ? "0" : to_string(expire_time - cur_ms_time);
        vector<string> cmd_vec = {"RESTORE", key, ttl_str, val};
        string request;
        build_request(cmd_vec, request);
        
        redis_conn.PipelineRawCmd(request);
        pipeline_cmd_cnt++;
        if (pipeline_cmd_cnt >= g_config.pipeline_cnt) {
            execute_pipeline(pipeline_cmd_cnt, redis_conn);
            pipeline_cmd_cnt = 0;
        }
    }
    
    if (pipeline_cmd_cnt > 0) {
        execute_pipeline(pipeline_cmd_cnt, redis_conn);
    }
    
    return 0;
}

int RdbReader::_LoadType()
{
    unsigned char type;
    if (fread(&type, 1, 1, rdb_file_) != 1) {
        log_message(kLogLevelError, "LoadType failed, file postion: %ld\n", ftell(rdb_file_));
        return -1;
    }
    return type;
}

time_t RdbReader::_LoadTime()
{
    int32_t t32;
    if (fread(&t32, 4, 1, rdb_file_) != 1) {
        log_message(kLogLevelError, "LoadTime failed, file postion: %ld\n", ftell(rdb_file_));
        return -1;
    }
    return (time_t)t32;
}

uint64_t RdbReader::_LoadMsTime()
{
    uint64_t t64;
    if (fread(&t64, 8, 1, rdb_file_) != 1) {
        log_message(kLogLevelError, "LoadMsTime failed, file postion: %ld\n", ftell(rdb_file_));
        return -1;
    }
    return t64;
}

/* Load an encoded length. The "is_encoded" argument is set to true if the length
 * is not actually a length but an "encoding type". See the REDIS_RDB_ENC_*
 * definitions in rdb_reader.h for more information. */
uint32_t RdbReader::_LoadLen(bool& is_encoded)
{
    unsigned char buf[2];
    uint32_t len;
    int type;
    
    is_encoded = false;
    if (fread(buf, 1, 1, rdb_file_) != 1) {
        log_message(kLogLevelError, "_LoadLen failed, read 1 byte file postion: %ld\n", ftell(rdb_file_));
        return REDIS_RDB_LENERR;
    }
    
    type = (buf[0] & 0xC0) >> 6;
    if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        is_encoded = true;
        return buf[0] & 0x3F;
    } else if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len. */
        return buf[0] & 0x3F;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (fread(buf + 1, 1, 1, rdb_file_) != 1) {
            log_message(kLogLevelError, "_LoadLen failed, read another 1 byte, file postion: %ld\n", ftell(rdb_file_));
            return REDIS_RDB_LENERR;
        }
        return ((buf[0] & 0x3F) << 8) | buf[1];
    } else {
        /* Read a 32 bit len. */
        if (fread(&len, 4, 1, rdb_file_) != 1) {
            log_message(kLogLevelError, "_LoadLen failed, read 4 byte, file postion: %ld\n", ftell(rdb_file_));
            return REDIS_RDB_LENERR;
        }
        
        return ntohl(len);
    }
}

string RdbReader::_LoadString()
{
    bool is_encoded;
    uint32_t len;
    
    len = _LoadLen(is_encoded);
    if (is_encoded) {
        switch(len) {
            case REDIS_RDB_ENC_INT8:
            case REDIS_RDB_ENC_INT16:
            case REDIS_RDB_ENC_INT32:
                return _LoadIntegerString(len);
            case REDIS_RDB_ENC_LZF:
                return _LoadLzfString();
            default:
                log_message(kLogLevelError, "Unknown RDB encoding type: %d\n", len);
                exit(1);
        }
    }
    
    if (len == REDIS_RDB_LENERR) {
        exit(1);
    }
    
    char* buf = new char[len];
    if (len && (fread(buf, 1, len, rdb_file_) != len)) {
        log_message(kLogLevelError, "LoadString failed\n");
        delete [] buf;
        exit(1);
    }
    
    string val(buf, len);
    delete [] buf;
    return val;
}

string RdbReader::_LoadSerializedValue(int rdbtype, uint64_t ttl, const string& key)
{
    string serialized_value;
    if (rdbtype == REDIS_RDB_TYPE_STRING) {
        string value = _LoadString();
        serialized_value = serialize_string(ttl, key, value);
    } else if (rdbtype == REDIS_RDB_TYPE_LIST) {
        size_t len;
        bool is_decoded;
        if ((len = _LoadLen(is_decoded)) == REDIS_RDB_LENERR) {
            log_message(kLogLevelError, "read list len failed\n");
            exit(1);
        }
        
        list<string> value_list;
        while(len--) {
            string element = _LoadString();
            value_list.push_back(element);
        }
        
        serialized_value = serialize_list(ttl, key, value_list);
    } else if (rdbtype == REDIS_RDB_TYPE_SET) {
        size_t len;
        bool is_decoded;
        if ((len = _LoadLen(is_decoded)) == REDIS_RDB_LENERR) {
            log_message(kLogLevelError, "read set len failed\n");
            exit(1);
        }
        
        set<string> member_set;
        while(len--) {
            string member = _LoadString();
            member_set.insert(member);
        }
        
        serialized_value = serialize_set(ttl, key, member_set);
    } else if (rdbtype == REDIS_RDB_TYPE_ZSET) {
        size_t zset_len;
        bool is_decoded;
        if ((zset_len = _LoadLen(is_decoded)) == REDIS_RDB_LENERR) {
            log_message(kLogLevelError, "read zset len failed\n");
            exit(1);
        }
        
        map<string, double> score_map;
        while(zset_len--) {
            string member = _LoadString();
            double score = _LoadDoubleValue();
            score_map[member] = score;
        }
        
        serialized_value = serialize_zset(ttl, key, score_map);
    } else if (rdbtype == REDIS_RDB_TYPE_HASH) {
        size_t hash_len;
        bool is_decoded;
        if ((hash_len = _LoadLen(is_decoded)) == REDIS_RDB_LENERR) {
            log_message(kLogLevelError, "read zset len failed\n");
            exit(1);
        }
        
        map<string, string> fv_map;
        while (hash_len--) {
            string field = _LoadString();
            string value = _LoadString();
            fv_map[field] = value;
        }
        
        serialized_value = serialize_hash(ttl, key, fv_map);
    } else if (rdbtype == REDIS_RDB_TYPE_HASH_ZIPMAP) {
        log_message(kLogLevelError, "deprecated RDB encodeing type: REDIS_RDB_TYPE_HASH_ZIPMAP\n");
        exit(1);
    } else if (rdbtype == REDIS_RDB_TYPE_LIST_ZIPLIST) {
        vector<string> vec;
        _LoadZipList(vec);
        serialized_value = serialize_list_vec(ttl, key, vec);
    } else if (rdbtype == REDIS_RDB_TYPE_SET_INTSET) {
        string intset_content = _LoadString();
        uchar_t* data = (uchar_t*)intset_content.data();
        int32_t data_size = (int32_t)intset_content.size();
        if (data_size < 8) {
            log_message(kLogLevelError, "intset content size too small: %d\n", data_size);
            exit(1);
        }
        
        set<string> member_set;
        int32_t encoding = _ReadInt32(data);
        int32_t length = _ReadInt32(data + 4);
        if (encoding * length + 8 != data_size) {
            log_message(kLogLevelError, "intset size not match, encoding=%d, length=%d, size=%d\n", encoding, length, data_size);
            exit(1);
        }
        int32_t offset = 8;
        while (offset < data_size) {
            if (encoding == INTSET_ENC_INT16) {
                int16_t value = _ReadInt16(data + offset);
                offset += 2;
                member_set.insert(to_string(value));
            } else if (encoding == INTSET_ENC_INT32) {
                int32_t value = _ReadInt32(data + offset);
                offset += 4;
                member_set.insert(to_string(value));
            } else if (encoding == INTSET_ENC_INT64) {
                int64_t value = _ReadInt64(data + offset);
                offset += 8;
                member_set.insert(to_string(value));
            } else {
                log_message(kLogLevelError, "unknown intset encoding: %d\n", encoding);
                exit(1);
            }
        }
        
        serialized_value = serialize_set(ttl, key, member_set);
    } else if (rdbtype == REDIS_RDB_TYPE_ZSET_ZIPLIST) {
        vector<string> vec;
        _LoadZipList(vec);
        serialized_value = serialize_zset_vec(ttl, key, vec);
    } else if (rdbtype == REDIS_RDB_TYPE_HASH_ZIPLIST) {
        vector<string> vec;
        _LoadZipList(vec);
        serialized_value = serialize_hash_vec(ttl, key, vec);
    } else if (rdbtype == REDIS_RDB_TYPE_LIST_QUICKLIST) {
        size_t len;
        bool is_decoded;
        if ((len = _LoadLen(is_decoded)) == REDIS_RDB_LENERR) {
            log_message(kLogLevelError, "read quicklist len failed\n");
            exit(1);
        }
        
        vector<string> vec;
        while (len--) {
            _LoadZipList(vec);
            
        }
        
        serialized_value = serialize_list_vec(ttl, key, vec);
    } else {
        log_message(kLogLevelError, "Unknown RDB encoding type %d\n", rdbtype);
        exit(1);
    }

    return serialized_value;
}

void RdbReader::_LoadZipList(vector<string>& vec)
{
    string content = _LoadString();
    uchar_t* data = (uchar_t*)content.data();
    int32_t data_size = (int32_t)content.size();
    int32_t zlbytes = _ReadInt32(data);
    if (zlbytes != data_size) {
        log_message(kLogLevelError, "ziplist len not match, zlbytes=%d, size=%d\n", zlbytes, data_size);
        exit(1);
    }
    //int32_t zltail = _ReadInt32(data + 4);
    int16_t zllen = _ReadInt16(data + 8);
    vec.reserve(zllen + vec.size());
    int32_t offset = 10;
    while ((zllen > 0) && (offset < data_size)) {
        // read length-prev-entry
        uint8_t len = data[offset];
        if (len <= 253) {
            offset++;
        } else {
            offset += 5;
        }
        
        // read flag and entry data
        uint8_t flag = data[offset++];
        if (ZIP_IS_STR(flag)) {
            uint8_t encoding = flag & ZIP_STR_MASK;
            int32_t len;
            if (encoding == ZIP_STR_06B) {
                len = flag & 0x3f;
            } else if (encoding == ZIP_STR_14B) {
                len = ((flag & 0x3f) << 8) | data[offset++];
            } else  {
                len = (data[offset] << 24) | (data[offset + 1] << 16) || (data[offset + 2] << 8) | data[offset + 3];
                offset += 4;
            }
            
            string entry((char*)data + offset, len);
            vec.push_back(entry);
            offset += len;
        } else if ((flag & ZIP_INT_MASK) == ZIP_INT_16B) {
            int16_t val = _ReadInt16(data + offset);
            offset += 2;
            vec.push_back(to_string(val));
        } else if ((flag & ZIP_INT_MASK) == ZIP_INT_32B) {
            int32_t val = _ReadInt32(data + offset);
            offset += 4;
            vec.push_back(to_string(val));
        } else if ((flag & ZIP_INT_MASK) == ZIP_INT_64B) {
            int64_t val = _ReadInt64(data + offset);
            offset += 8;
            vec.push_back(to_string(val));
        } else if (flag == ZIP_INT_24B) {
            int32_t val = _ReadInt24(data + offset);
            offset += 3;
            vec.push_back(to_string(val));
        } else if (flag == ZIP_INT_8B) {
            int8_t val = data[offset++];
            vec.push_back(to_string(val));
        } else {
            int8_t val = (flag & 0xf) - 1;
            vec.push_back(to_string(val));
        }
        
        zllen--;
    }
    
    if (data[offset] != ZIP_END) {
        log_message(kLogLevelError, "ziplist not end with 0xFF, offset=%d, zlbytes=%d\n", offset, zlbytes);
        exit(1);
    }
}

string RdbReader::_LoadIntegerString(int enctype)
{
    unsigned char enc[4];
    long long val;
    
    if (enctype == REDIS_RDB_ENC_INT8) {
        if (fread(enc, 1, 1, rdb_file_) != 1) {
            exit(1);
        }
        
        val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (fread(enc, 2, 1, rdb_file_) != 1) {
            exit(1);
        }
        
        v = enc[0] | (enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (fread(enc, 4, 1, rdb_file_) != 1) {
            exit(1);
        }
        
        v = enc[0] | (enc[1]<<8) | (enc[2]<<16) | (enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        log_message(kLogLevelError, "Unknown RDB integer encoding type\n");
        exit(1);
    }

    return std::to_string(val);
}

string RdbReader::_LoadLzfString()
{
    unsigned int len, clen;
    unsigned char *cbuf = NULL;
    unsigned char *buf = NULL;
    bool is_encoded;
    
    if ((clen = _LoadLen(is_encoded)) == REDIS_RDB_LENERR) {
        exit(1);
    }
    
    if ((len = _LoadLen(is_encoded)) == REDIS_RDB_LENERR) {
        exit(1);
    }
    
    cbuf = new unsigned char[clen];
    buf = new unsigned char [len];

    if (fread(cbuf, 1, clen, rdb_file_) != clen) {
        log_message(kLogLevelError, "read compress string failed\n");
        exit(1);
    }
    
    if (lzf_decompress(cbuf, clen, buf, len) == 0) {
        log_message(kLogLevelError, "lzf_decompress failed\n");
        exit(1);
    }
    
    string val((char*)buf, len);
    delete [] cbuf;
    delete [] buf;
    return val;
}

double RdbReader::_LoadDoubleValue()
{
    char buf[256];
    unsigned char len;
    
    if (fread(&len, 1, 1, rdb_file_) != 1) {
        log_message(kLogLevelError, "LoadDoubleValue failed\n");
        exit(1);
    }
    
    switch (len) {
        case 255:
            return -1.0 / 0.0; // negative infinity
        case 254:
            return 1.0 / 0.0; // positive infinity
        case 253:
            return 0.0 / 0.0; // nan
        default:
            if (fread(buf, 1, len, rdb_file_) != len) {
                log_message(kLogLevelError, "LoadDoubleValue failed\n");
                exit(1);
            }
            
            double value;
            buf[len] = '\0';
            sscanf(buf, "%lg", &value);
            return value;
    }
}

int16_t RdbReader::_ReadInt16(uchar_t* buf)
{
    return buf[0] | (buf[1] << 8);
}

int32_t RdbReader::_ReadInt32(uchar_t* buf)
{
    return buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
}

int64_t RdbReader::_ReadInt64(uchar_t* buf)
{
    return (int64_t)buf[0] | ((int64_t)buf[1] << 8) | ((int64_t)buf[2] << 16) | ((int64_t)buf[3] << 24) |
        ((int64_t)buf[4] << 32) | ((int64_t)buf[5] << 40) | ((int64_t)buf[6] << 48) | ((int64_t)buf[7] << 56);
}

int32_t RdbReader::_ReadInt24(uchar_t* buf)
{
    return buf[0] | (buf[1] << 8) | (buf[2] << 16);
}
