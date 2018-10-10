SparkSQL是Spark上的高级模块，SparkSQL是一个SQL解析引擎，将SQL解析成特殊的RDD（DataFrame），然后在Spark集群中运行

- SparkSQL是用来处理结构化数据的（先将非结构化的数据转换成结构化数据）

- SparkSQL支持两种编程API
	1.SQL方式
	2.DataFrame的方式（DSL）

- SparkSQL兼容hive（元数据库、SQL语法、UDF、序列化、反序列化机制）

- SparkSQL支持统一的数据源，课程读取多种类型的数据

- SparkSQL提供了标准的连接（JDBC、ODBC），以后可以对接一下BI工具

#### RDD和DataFrame的区别

DataFrame里面存放的结构化数据的描述信息，DataFrame要有表头（表的描述信息），描述了有多少列，每一列数叫什么名字、什么类型、能不能为空

DataFrame是特殊的RDD（RDD+Schema信息就变成了DataFrame）

### API编程
SparkSQL 1.x和2.x的编程API有一些变化，企业中都有使用，所以两种方式都将

#### 先用1.x的方式：
##### SQL方式
		创建一个SQLContext
			- 创建sparkContext，然后再创建SQLContext
			- 先创建RDD，对数据进行整理，然后关联case class，将非结构化数据转换成结构化数据
			- 调用sqlContext的createDataFrame方法将RDD转换成DataFrame
			- 注册临时表
			- 执行SQL（Transformation，lazy）
			- 执行Action
      
      ----
			- 创建sparkContext，然后再创建SQLContext
			- 先创建RDD，对数据进行整理，然后关联Row，将非结构化数据转换成结构化数据
			- 定义schema
			- 调用sqlContext的createDataFrame方法
			- 注册临时表
			- 执行SQL（Transformation，lazy）
			- 执行Action
```java
//变成DF后就可以使用两种API进行编程了
//把DataFrame先注册临时表
bdf.registerTempTable("t_Boy");

//书写SQL（SQL方法应其实是Transformation， 一个DataFrame转换成另一个DataFrame）
Dataset<Row> result = sqlContext.sql("SELECT * FROM t_boy ORDER BY fv desc, age asc");
```
      
##### DSL（DatFrame API）
```java
//不使用SQL的方式，就不用注册临时表了
 Dataset<Row> select = bdf.select("name", "age", "fv");
 Dataset<Row> ordered = select.orderBy(bdf.col("fv").desc(), bdf.col("age").asc());
```


