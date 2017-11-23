build () {
	rocksdb_version=`ls 3rd_party/rocksdb-*.tar.gz|awk -F"-" '{print $2}'|awk -F"." '{print $1"."$2"."$3}'`
	if [ ! -d ./3rd_party/rocksdb-$rocksdb_version ]; then
		cd 3rd_party
		tar zxvf rocksdb-$rocksdb_version.tar.gz
		
		cd rocksdb-$rocksdb_version
		make static_lib
		
		cd ../..
	fi

	cd base
	make ver=release -j
	cd ../server
	make ver=release version=$1 rocksdb_ver=$rocksdb_version -j
}

clean() {
	rocksdb_version=`ls 3rd_party/rocksdb-*.tar.gz|awk -F"-" '{print $2}'|awk -F"." '{print $1"."$2"."$3}'`
	cd base
	make clean
	cd ../server
	make clean rocksdb_ver=$rocksdb_version
}

case $1 in
	clean)
		clean
		;;
	version)
		build $2
		;;
	*)
		build
		;;
esac

