### Kafka的一些概念
- Broker ： 安装Kafka服务的那台集群就是一个broker（broker的id要全局唯一）
- Producer ：消息的生产者，负责将数据写入到broker中（push）
- Consumer：消息的消费者，负责从kafka中读取数据（pull），老版本的消费者需要依赖zk，新版本的不需要
- Topic: 主题，相当于是数据的一个分类，不同topic存放不同的数据
- Consumer Group： 消费者组，一个topic可以有多个消费者同时消费，多个消费者如果在一个消费者组中，那么他们不能重复消费数据

Spark Streaming 2.2.0兼容kafka 0.8.2.1以上的版本，主要支持0.8和0.10这两个版本


### kafka集群安装
	1.下载Kafka安装包
	2.上传安装包
	3.解压
	4.修改配置文件 config/server.properties
		broker.id=0
		host.name=node-4
		log.dirs=/data/kafka
		zookeeper.connect=node-1:2181,node-2:2181,node-3:2181
	5.将配置好的kafka拷贝到其他机器上
	6.修改broker.id和host.name
	7.启动kafka
		/bigdata/kafka_2.11-0.8.2.2/bin/kafka-server-start.sh -daemon /bigdata/kafka_2.11-0.8.2.2/config/server.properties 


- 查看topic信息
/bigdata/kafka_2.11-0.8.2.2/bin/kafka-topics.sh --list --zookeeper node-1:2181,node-2:2181

- 创建topic
/bigdata/kafka_2.11-0.8.2.2/bin/kafka-topics.sh --create --zookeeper node-1:2181,node-2:2181 --replication-factor 3 --partitions 3 --topic xiaoniu

- 往Kafka的topic中写入数据(命令行的生成者)
/bigdata/kafka_2.11-0.8.2.2/bin/kafka-console-producer.sh --broker-list node-4:9092,node-5:9092,node-5:9092 --topic xiaoniu

- 启动消费者
/bigdata/kafka_2.11-0.8.2.2/bin/kafka-console-consumer.sh --zookeeper node-1:2181,node-2:2181 --topic xiaoniu --from-beginning
	
	
kafka进程部分主从，但是分区有leader和follower之分，leader分区负责数读写，follwer分区负责同步数据。将读写的压力均摊到多台机器上，从而提高了读写能力。
