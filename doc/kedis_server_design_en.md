## 1. Introduction
Kedis-Server is a persistence kv storage server with [RocksDB](https://github.com/facebook/rocksdb) as its storage engine. It is compatible with [Redis](https://redis.io/) protocol and support most Redis commands，so all Redis client can be used to communicate with Kedis-Server

The main reason to develop Kedis-Server is that a single Redis's capacity is limited by the machine's memory, by use SSD or disk as a storage device, we can get much larger capacity and reduce storage cost

## 2. Network and Thread Model
Kedis-Server's network model is multi-thread IO reactor model，refer to Doug Lea's [Scalable IO in Java](http://gee.cs.oswego.edu/dl/cpjslides/nio.pdf) for details

* The main IO thread is an EventLoop，for listening client connection
* A network IO thread pool，every thread is an EventLoop，all network IO and client request commands are executed in these IO threads
* Two background processing threads: one expire thread to delete expired keys and one binlong thread to delete too old binlog command

Here is the thread modle graph:

<img width="400" height="300" src="images/thread_model.png"/>

## 3. Architecture

<img width="500" height="450" src="images/kedis_arch.png"/>

## 4. Storage Encoding

RocksDB is a simple key-value storage engine, so to implement Redis complex structue, we must use some encoding method

Briefly, every redis type has a meta data key-value pair, represent data type, TTL and element count. The actual element data is store as data key-value pair (String type has only meta data kv pair) 


### Some Convection

type use 1 byte

all integer type use **big endian**

len is used as length of key or value, use variant length encoding：

	if the highest bit is 0, use the left 7 bits to represent length
	if the highest two bit is 10, use the left 14 bits to represent lenth
	if the highest tow bit is 11, use the left 30 bits to represent lenth

ttl use 8 bytes, as a milliseconds timestamp，if 0, mean no expire setted for the key

count use 8 bytes，represent element count in a complex structure

sequence use 8 bytes

### 4.1 String Type
<img width="400" height="80" src="images/string.png"/>


RocksDB's encoding for key have type, key fields

* type=1，represent KEY_TYPE_META
* key is the actual key string

All type's meta data encoding key all use this format, so key will not replicate in the same kedis-server DB, the actual Redis type is in field of the encoding value 

key is not prefix with len field，so all keys are in lexicographical order

RocksDB's encoding for value have 4 fields: type，ttl，len，value

* type=2，represent KEY_TYPE_STRING
* ttl is the expire timestamp in milliseconds
* len is the length of the value
* value is the actual value string

### 4.2 Hash Type
<img width="400" height="210" src="images/hash.png"/>

RocksDB's encoding for meta key have type, key，the same with string type

RocksDB's encoding for meta value have 3 fields: type, ttl and count

* type=3, represent KEY_TYPE_HASH
* ttl is the expire timestamp in milliseconds
* count is the element count in the hash key，so redis command hlen can be implemented with just one RocksDB read

Hash type has element key-value pairs to represent every hash element

RocksDB's encoding for element key have 5 fields: type, len, key, len, field

* type=4, represent KEY_TYPE_HASH_FIELD
* the first len is the length of the key
* key is the key string of the hash structure
* the second len is the length of the field
* field is an element of the hash structure

RocksDB's encoding for element value include type, len, value 

* type=4, represent KEY_TYPE_HASH_FIELD
* len is the length of the value
* value is the actual value string in the hash structure


### 4.3 List Type
<img width="400" height="260" src="images/list.png"/>

RocksDB's encoding for meta key have type, key，the same with string type

RocksDB's encoding for meta value have 6 fields: type, ttl, cout, head, tail, seq

* type=5, represent KEY_TYPE_LIST
* ttl is the expire timestamp in milliseconds
* count is the element count, so redis command llen can be implement with one Rocksdb read
* head is the head sequence of the list, use 8 bytes
* tail is the tail sequence of the list, use 8 bytes
* seq is the sequence number used for new element, and need increment after add a new element

RocksDB's encoding for element key have type, len, key and seq 

* type=6, represent KEY_TYPE_LIST_ELEMENT
* len the length of the key
* key is the key of the list structure
* seq is the sequence of the element，use 8 bytes

RocksDB's encoding for element value have five fields: type, prev, next, len and value

* type=6, represent KEY_TYPE_LIST_ELEMENT
* prev is the sequence of the previous element
* next is the sequence of the next element
* len is the length of the value
* value is the actual element value string

### 4.4 Set Type
<img width="400" height="150" src="images/set.png"/>

RocksDB's encoding for meta key have type, key，the same with string type

RocksDB's encoding for meta value have type, ttl and count

* type=7, represent KEY_TYPE_SET
* ttl is the epxire timestamp
* count is the element count of the set, so redis commmand scard can be implemented with one RocksDB read

RocksDB's encoding for element key have five fields: type, len, key, len, member

* type=8, represent KEY_TYPE_SET_MEMBER
* the first len is the length of the key
* key is the key of the set structure
* the second len the length of the member
* member is a element string in the set structure

RocksDB's encoding for element value has only one field: type

* type=8, reprenset KEY_TYPE_SET_MEMBER

### 4.5 Sorted Set Type
<img width="400" height="220" src="images/zset.png"/>


RocksDB's encoding for meta key have type, key，the same with string type

RocksDB's encoding for meta value have type, ttl and count

* type=9, represent KEY_TYPE_ZSET
* ttl is the expire timestamp
* count is the element count of zset

zset is a much more complicated structure, cause it need two kv to represent its actual element data, one for get score through member and the another for implement member ordering

RocksDB's encoding for score key have five fields: type, len, key, len, member

* type=10, represent KEY_TYPE_ZSET_SCORE
* the first len is the length of the key
* key is the key of the zset
* the second len is the length of member
* member is the element string of zset

RocksDB's encoding for score value have type, score

* type=10，represent KEY_TYPE_ZSET_SCORE
* score is an encoding integer value with 8 bytes

RocksDB's encoding for sort key have 5 fields: type, len, key, score, member

* type=11, represent KEY_TYPE_ZSET_SORT
* len is the length of the zset key
* key is the key of zset
* score is an enoding integer of 8 bytes
* member is the element string of zset

RocksDB's encoding for sort value have only one field type
* type=11，represent KEY_TYPE_ZSET_SORT

Score in the encoding and zset have a transfer relationship， see [zset double support](zset_double_score_design.md)

When all score are the same, member will be ordered by lexicographical ordering

### 4.6 TTL
<img width="400" height="80" src="images/ttl.png"/>

RocksDB's encoding for key have type, ttl, len and key

* type=12, represent KEY_TYPE_TTL
* ttl is the expire timestamp
* len is the length of key
* key is the key of the redis structure

RocksDB's encoding for value have only one type
* type represent the redis structure type

This encoding is used for actively expire keys

## 5. Replication
Master-Slave replication implement a state machine to fulfill full and increment synchronization, the main procedure is as follows:

<img width="500" height="600" src="images/replication-state.png"/>

1. Slave send "AUTH password" to master if masterauth is not empty，enter into RECV_AUTH state，otherwise goto step 3
2. Slave receive OK response
3. Slave send "REPLCONF listening-port port" to master，enter into RECV_PORT state
4. Slave receive OK response
5. Slave send "PSYNC binlog_id expect_binlog_seq" to master，enter into RECV_SYNC state 
6. If slave receive CONTINUE response，means master have slave's request binlog，slave enter into CONNECTED statem and increment synchronization phase start
7. If slave receive FULLRESYNC response，means master will send full snapshot，slave enter into RECV_SNAPSHOT state，then master will send multiple "SELECT db" and "RESTORE key ttl serialized-value" commands，until slave receive "REPL_SNAPSHOT_COMPLETE binlog_id cur_db_idx max_binlog_seq" command, represent slave received all snapshot data，slave enter into CONNECTED state，and increment synchronization phase start


## 6. Implemention Details
* Use RocksDB ColumnFamily to implement multiple db
* Cause most redis command will be implemented with multiple RocksDB write, if failed in halfway, the DB will be corrupted, so We use RocksDB's WiteBatch to implement these writes atomically
* Most code of HyperLogLog was migrated from Redis codebase, need to read these papers in the comment to understand the principle

## 7. Roadmap
* Support most common Redis commands (finish)
* Master-Salve replication (finish)
* Transfer partial Redis tcl test case (finish)
* Server performance optimization 
* Support all Redis commands
* Data migration tools
	* kedis_port: data migration tool between two kedis-server (finish)
	* redis_to_kedis: data migration tool from redis to kedis-server (finish)
	* kedis_to_redis: data migration tool from kedis-server to redis (finish)
	
