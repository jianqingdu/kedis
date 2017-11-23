这里包括了那些不会被tests测试脚本测到的测试用例

* master运行时，用redis-benchmark -p 6381 -n 1000000 -r 200000000 -d 128 -c 60 -t mset不停的插入数据，然后挂载一个slave上去，等slave连上master后，关闭插入benchmark，等数据全部同步后，查看master和slave上的数据量是否一致
* 关闭master，测试slave是否会一直重连
* 修改slave的配置文件，把master的ip地址改成一个其他网段不存在的地址，启动slave，测试是否到master的连接在2秒后重连 
