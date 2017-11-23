## info keyspace的设计实现
由于以下3个原因，需要特别的设计来实时查询DB里面的key的个数

* 当key个数很大时，实时遍历每个key来统计太耗费时间
* RocksDB的GetIntProperty(cf_handle, “rocksdb.estimate-num-keys")接口只能估算key的大概个数
* 复杂数据结构的一个key对应到RocksDB的多个key

大概的思路如下

* 用一个内存计数器来存储当前key的个数，每当有key删除和添加时，更新该计数器
* 每隔1秒把该计数器写入到RocksDB进行持久化
* 服务启动时，读取RocksDB里面的计数器，如果计数器个数小于10万，则遍历DB的每个key，来获取精确的key个数，否则就以读取的计数器做为当前key的个数，这样的目的是，当key个数比较少时，让统计尽量精确，当key个数比较大时，有点误差也是可以接受的
* 服务结束时，把计数器写入RocksDB，这样只有当服务进程异常crash，或者机器掉电，而且key个数比较大时，才会出现key计数器不是特别准的情况


