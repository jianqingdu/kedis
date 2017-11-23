## zset数据结构和score的关系
在Redis里面zset是一种有序集合，一个zset的key下面，所有(score，member)的元素，都按score排序，这样可以获取一个区间的所有元素，比如通过ZRANGEBYSCORE命令来获取[score1, score2]这个区间内的所有元素

RocksDB只支持kv结构，而且key是按字节序排序的，所有为了实现zset数据类型，在RocksDB上的key必须用下面的方式来编码：

	key:score:member

要是score的编码符合字节序和score的double数值大小顺序一致，就可以实现zset的数据结构

## double类型的score的编码方式
按照IEEE-754标准，double类型占用8个字节，按大端字节序分成3个部分：

* 符号: 1 bit （0为正，1为负）
* 指数: 11 bits
* 分数: 52 bits

可以参考这个[wiki](https://en.wikipedia.org/wiki/Double-precision_floating-point_format)

对于正数部分，按照IEEE-754的编码，如果用大端字节序保存，则字节序和double的大小顺序一致。对于负数部分，如果用大端字节序保存，则字节序和double的顺序刚好相反。所以采用如下的编码方式，则所有double类型的字节序和大小一致：

	uint64_t double_to_uint64(double d)
	{
    	uint64_t u = *(uint64_t*)&d;
    	if (d >= 0) {
        	u |= 0x8000000000000000;
    	} else {
        	u = ~u;
    	}
    
    	return u;
	}

	double uint64_to_double(uint64_t u)
	{
    	if ((u & 0x8000000000000000) > 0) {
        	u &= ~0x8000000000000000;
    	} else {
        	u = ~u;
    	}
    
    	return *(double*)&u;
	}
	
就是对于所有正数，最高位置1，对于所以负数，则取反。
这样处理后正数的最高位是1，负数的最高位是0，所以按字节序，所有正数肯定大于所有负数
，所有正数按字节序排序，所有负数也按字节序排序
