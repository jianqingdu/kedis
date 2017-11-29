build () {
	cd src
	# build RocksDB library
	rocksdb_version=`ls 3rd_party/rocksdb-*.tar.gz|awk -F"-" '{print $2}'|awk -F"." '{print $1"."$2"."$3}'`
	if [ ! -d ./3rd_party/rocksdb-$rocksdb_version ]; then
		cd 3rd_party
		tar zxvf rocksdb-$rocksdb_version.tar.gz
		
		cd rocksdb-$rocksdb_version
		make static_lib
		
		cd ../..
	fi

	# build kedis-server
	cd base
	make ver=release -j 4
	cd ../server
	make ver=release rocksdb_ver=$rocksdb_version -j 4

	# build tools
	cd ../../tools/kedis_port
	make -j 4
	cd ../kedis_to_redis
	make -j 4
	cd ../redis_to_kedis
	make -j 4

	# make package
	cd ../..
	kedis_version=`grep major src/server/kedis_version.h |awk '{print $3}'|sed 's/\"//g'`
	package_dir="kedis-$kedis_version"
	mkdir $package_dir
	cp src/server/kedis-server src/server/kedis.conf $package_dir
	cp tools/kedis_port/kedis_port $package_dir
	cp tools/kedis_to_redis/kedis_to_redis $package_dir
	cp tools/redis_to_kedis/redis_to_kedis $package_dir
	tar zcvf $package_dir.tar.gz $package_dir
	rm -fr $package_dir
}

clean() {
	cd src
	rocksdb_version=`ls 3rd_party/rocksdb-*.tar.gz|awk -F"-" '{print $2}'|awk -F"." '{print $1"."$2"."$3}'`
	cd base
	make clean
	cd ../server
	make clean rocksdb_ver=$rocksdb_version

	cd ../../tools/kedis_port
	make clean
	cd ../kedis_to_redis
	make clean
	cd ../redis_to_kedis
	make clean
	cd ../hiredis
	make clean
}

case $1 in
	clean)
		clean
		;;
	*)
		build
		;;
esac

