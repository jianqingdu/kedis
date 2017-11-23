//
//  redis_byte_stream.h
//  kedis
//
//  Created by ziteng on 16-6-14.
//  Copyright (c) 2016年 mgj. All rights reserved.
//

#ifndef __REDIS_BYTE_STREAM_H__
#define __REDIS_BYTE_STREAM_H__

#include "util.h"

enum {
    PARSE_REDIS_ERROR_NO_MORE_DATA = 0,
    PARSE_REDIS_ERROR_INVALID_FORMAT = 1,
};

class ParseRedisException {
public:
	ParseRedisException(uint32_t code, const string& msg) : error_code_(code), error_msg_(msg) { }
	virtual ~ParseRedisException() {}
    
    uint32_t GetErrorCode() { return error_code_; }
	char* GetErrorMsg() { return (char*)error_msg_.c_str(); }
private:
    uint32_t    error_code_;
	string      error_msg_;
};

class RedisByteStream {
public:
    RedisByteStream(char* data, int len);
    virtual ~RedisByteStream();
    
    int GetOffset() { return offset_; }
    int ReadByte();
    void ReadLine(string& line);
    void ReadBytes(int size, string& value);
private:
    char*   data_;
    int     len_;
    int     offset_; // current read position
};

#endif
