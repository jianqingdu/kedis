MIGRATE命令格式

	MIGRATE host port key destination_db timeout [use_prefix]
	
参数说明

* host 迁移的目的IP
* port 迁移的目的端口
* key 需要迁移的一个key或者key的前缀，具体看use_prefix参数
* destination_db 迁移的目的db号
* timeout 超时时间，如果有错误或者迁移超时，会返回IOERROR错误
* use_prefix 可选，如果存在，表示migrate迁移一个以key为前缀的任何一个key，这个主要是为了支持扩缩容迁移工具

返回值

* 当key迁移成功是，返回OK状态
* 当key不存在时，返回NOKEY错误
* 当发生错误时，比如超时，返回IOERROR错误

Redis的migrate命令是同步式的，但由于Redis是操作内存数据，所有很快会返回，但是由于kedis-server是基于RocksDB实现的，对于那些复杂数据结构的迁移，可能需要比较长的时间，所以需要实现异步的方式，来减少对其他命令的影响

kedis-server需要维持到host:port的长连接，用于减少每次migrate命令的连接建立消耗，同时也用于保存实现异步migrate命令的上下文信息：
	
	struct MigrateContext {
		int from_handle;
		int timeout;
		string key;
	}; 
	
	struct MigrateServer {
		int handle;
		int cur_db_index;
		list<MigrateContext*> request_list;
	};
	
kedis-server每次收到一个migrate请求，检查是否有对应的MigrateServer，如果没有，则建立连接，然后把MigrateContext加入request_list里面，获取key的所有数据序列化，发送RESTORE请求，异步收到回复后，取出第一个请求的MigrateContext，然后发送回复给其中的from_handle

key数据的序列化格式，就用kv在RocksdB的encoding方式来序列化

需要定时器检查所有MigrateContext的超时情况


