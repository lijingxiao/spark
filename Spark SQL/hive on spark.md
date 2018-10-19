Hive On Spark （跟hive没太的关系，就是使用了hive的标准（HQL， 元数据库、UDF、序列化、反序列化机制））

Hive原来的计算模型是MR,有点慢（将中间结果写入到HDFS中）

Hive On Spark 使用RDD（DataFrame），然后运行在spark 集群上

真正要计算的数据是保存在HDFS中，mysql这个元数据库，保存的是hive表的描述信息，描述了有哪些database、table、以及表有多少列，每一列是什么类型，还要描述表的数据保存在hdfs的什么位置。

- hive跟mysql的区别？

hive是一个数据仓库（存储数据并分析数据，分析数据仓库中的数据量很大，一般要分析很长的时间）
mysql是一个关系型数据库（关系型数据的增删改查（低延迟））


- hive的元数据库中保存怎知要计算的数据吗？
	不保存，保存hive仓库的表、字段、等描述信息

- 真正要计算的数据保存在哪里了？
	保存在HDFS中
  
- hive的元数据库的功能
	建立了一种映射关系，执行HQL时，先到MySQL元数据库中查找描述信息，然后根据描述信息生成任务，然后将任务下发到spark集群中执行

- spark sql 整合hive，将hive的sql写在一个文件中执行（用-f这个参数）
/bigdata/spark-2.2.0-bin-hadoop2.7/bin/spark-sql --master xxx --driver-class-path mysql-connector-java-5.1.7-bin.jar -f hive-sqls.sql

在idea中开发，整合hive
```
 <!-- spark如果想整合Hive，必须加入hive的支持 -->
	<dependency>
	    <groupId>org.apache.spark</groupId>
	    <artifactId>spark-hive_2.11</artifactId>
	    <version>2.2.0</version>
	</dependency>

    //如果想让hive运行在spark上，一定要开启spark对hive的支持
    val spark = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      .enableHiveSupport()//启用spark对hive的支持(可以兼容hive的语法了)
      .getOrCreate()
```
