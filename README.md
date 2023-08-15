# Kedis-Server -- A Redis Compatible persistance Nosql

Kedis-Server is a Redis-Protocol compatible persistance NoSQL with RocksDB as its storage engine, support most Redis comamnds and those Reids complex data structure: string, list, hash, set, sorted set and hyperloglog

## Build
Platform
	
	Linux 
	Mac OS X (intel)
	
Dependency:

	C++11 compiler, nothing more
	recommend install jemalloc, snappy for performance improvement

How to build 

	./build.sh
	
If everything is OK, the executable file will be in src/server/kedis-server.
The server config file is in src/server/kedis.conf

## Run

	./kedis-server -c ./kedis.conf

After that, you can connect to server with any redis client, suck as redis-cli

	redis-cli -p 6379 set k v
	redis-cli -p 6379 get k

## Run The Test

After build the server, you can run the test case with 

	./runtest.sh

## Supported Commands

[Currrent supported Commands](doc/support_command.md)

## Architecture

[Kedis-Server Deisgn Documentation](doc/kedis_server_design_en.md)
