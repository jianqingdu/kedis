/*
 * simple_buffer.cpp
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#include "simple_buffer.h"
#include <stdlib.h>
#include <string.h>

SimpleBuffer::SimpleBuffer()
{
	m_buffer = NULL;
    
	m_alloc_size = 0;
	m_write_offset = 0;
    m_read_offset = 0;
}

SimpleBuffer::~SimpleBuffer()
{
    if (m_buffer) {
		free(m_buffer);
		m_buffer = NULL;
	}
    
	m_alloc_size = 0;
	m_write_offset = 0;
    m_read_offset = 0;
}

void SimpleBuffer::Extend(uint32_t len)
{
	m_alloc_size = m_write_offset + len;
	m_alloc_size += m_alloc_size >> 2;	// increase by 1/4 allocate size
	uchar_t* new_buf = (uchar_t*)realloc(m_buffer, m_alloc_size);
	m_buffer = new_buf;
}

uint32_t SimpleBuffer::Write(void* buf, uint32_t len)
{
	if (m_write_offset + len > m_alloc_size) {
		Extend(len);
	}
    
	if (buf) {
		memcpy(m_buffer + m_write_offset, buf, len);
	}
    
	m_write_offset += len;
    
	return len;
}

uint32_t SimpleBuffer::Read(void* buf, uint32_t len)
{
	if (len > GetReadableLen())
		len = GetReadableLen();
    
	if (buf)
		memcpy(buf, m_buffer + m_read_offset, len);
    
	m_read_offset += len;
	return len;
}

// move the rest of data to the start position of the buffer, reset read_offset and write_offset
void SimpleBuffer::ResetOffset()
{
    if (m_read_offset == 0)
        return;
    
    if (m_read_offset < m_write_offset)
        memmove(m_buffer, m_buffer + m_read_offset, GetReadableLen());
    
    m_write_offset = GetReadableLen();
    m_read_offset = 0;
}

void SimpleBuffer::Clear()
{
    m_write_offset = m_read_offset = 0;
    m_alloc_size = 0;
    if (m_buffer) {
        free(m_buffer);
        m_buffer = NULL;
    }
}
