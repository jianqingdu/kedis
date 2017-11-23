/*
 * byte_stream.h
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#ifndef __BASE_BYTE_STREAM_H__
#define __BASE_BYTE_STREAM_H__

#include "ostype.h"
#include "util.h"
#include "simple_buffer.h"

class ParseException {
public:
    ParseException(const char* error_msg) {
        error_msg_ = error_msg;
    }
    virtual ~ParseException() {}
    
    char* GetErrorMsg() { return (char*)error_msg_.c_str(); }
private:
    string		error_msg_;
};

class ByteStream
{
public:
    ByteStream();
	ByteStream(uchar_t* buf, uint32_t len);
	ByteStream(SimpleBuffer* pSimpBuf, uint32_t pos = 0);
    ByteStream(string* pStr);
	~ByteStream() {}
    
    void SetBuf(uchar_t* buf, uint32_t len) { m_pBuf = buf; m_len = len; }
    void SetSimpleBuffer(SimpleBuffer* buf) { m_pSimpBuf = buf; }
    void SetString(string* buf) { m_pStr = buf; }
	unsigned char* GetBuf() {
        if (m_pSimpBuf) {
            return m_pSimpBuf->GetBuffer();
        } else if (m_pStr) {
            return (uchar_t*)m_pStr->data();
        } else {
            return m_pBuf;
        }
    }
	uint32_t GetPos() { return m_pos; }
	uint32_t GetLen() { return m_len; }
	void Skip(uint32_t len) {
		m_pos += len;
		if (m_pos > m_len) {
			throw ParseException("skip passed boundary");
		}
	}
    
	static int16_t ReadInt16(uchar_t* buf);
	static uint16_t ReadUint16(uchar_t* buf);
	static int32_t ReadInt32(uchar_t* buf);
	static uint32_t ReadUint32(uchar_t* buf);
    static int64_t ReadInt64(uchar_t* buf);
    static uint64_t ReadUint64(uchar_t* buf);
	
    static void WriteInt16(uchar_t* buf, int16_t data);
	static void WriteUint16(uchar_t* buf, uint16_t data);
	static void WriteInt32(uchar_t* buf, int32_t data);
	static void WriteUint32(uchar_t* buf, uint32_t data);
    static void WriteInt64(uchar_t* buf, int64_t data);
    static void WriteUint64(uchar_t* buf, uint64_t data);
    
    void operator << (int8_t data);
    void operator << (uint8_t data);
    void operator << (int16_t data);
    void operator << (uint16_t data);
    void operator << (int32_t data);
    void operator << (uint32_t data);
    void operator << (int64_t data);
    void operator << (uint64_t data);
    void operator << (const string& str);
    
    void operator >> (int8_t& data);
    void operator >> (uint8_t& data);
    void operator >> (int16_t& data);
    void operator >> (uint16_t& data);
    void operator >> (int32_t& data);
	void operator >> (uint32_t& data);
    void operator >> (int64_t& data);
    void operator >> (uint64_t& data);
    void operator >> (string& str);
    
    void WriteVarUInt(uint32_t len);
    uint32_t ReadVarUInt();
    
	void WriteData(uchar_t* data, uint32_t len);
	uchar_t* ReadData(uint32_t& len);
    
    void WriteStringWithoutLen(const string& str);
    void ReadStringWithoutLen(string& str);
private:
	void _WriteByte(void* buf, uint32_t len);
	void _ReadByte(void* buf, uint32_t len);
private:
	SimpleBuffer*	m_pSimpBuf;
    string*         m_pStr;
	uchar_t*		m_pBuf;
	uint32_t		m_len;
	uint32_t		m_pos;
};

#endif
