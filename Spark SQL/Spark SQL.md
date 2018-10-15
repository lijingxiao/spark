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

#### 1.x的方式：
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

#### 2.x的方式：
sparkSession
```java
SparkSession spark = SparkSession.builder().appName("ipLocation").master("local[4]").getOrCreate();

//需要使用javaSparkContext时
JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
```

#### sparksql方式实现IP归属地统计
- 注册两个view，v_rules和v_log，进行不等值join
```java
ArrayList<StructField> structFields = new ArrayList<>();
structFields.add(DataTypes.createStructField("ipStart", DataTypes.LongType, true));
structFields.add(DataTypes.createStructField("ipEnd", DataTypes.LongType, true));
structFields.add(DataTypes.createStructField("province", DataTypes.StringType, true));
StructType ruleSchema = DataTypes.createStructType(structFields);

Dataset<Row> ruleDf = spark.createDataFrame(rulesRDD, ruleSchema);
try {
    ruleDf.createTempView("v_rules");
} catch (AnalysisException e) {
    e.printStackTrace();
}

ArrayList<StructField> structFieldsLog = new ArrayList<>();
structFieldsLog.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
StructType schemaLog = DataTypes.createStructType(structFieldsLog);
Dataset<Row> logDf = spark.createDataFrame(ipLogRDD, schemaLog);
try {
    logDf.createTempView("v_log");
} catch (AnalysisException e) {
    e.printStackTrace();
}

Dataset<Row> result = spark.sql("SELECT province, count(*) counts FROM v_log JOIN v_rules ON (ip >= ipStart AND ip <= ipEnd) GROUP BY province ORDER BY counts DESC");

```
- 广播规则，自定义udf，注册v_log
```java
Broadcast<List<Tuple3<Long, Long, String>>> broadcast = jsc.broadcast(rules);
//自定义UDF
spark.udf().register("ip2Province", new UDF1<Long, String>() {
    @Override
    public String call(Long ip) throws Exception {
        //查找ip规则（事先已经广播了，已经在Executor中了）
        //函数的逻辑是在Executor中执行的，怎样获取ip规则的对应的数据呢？
        //使用广播变量的引用，就可以获得
        List<Tuple3<Long, Long, String>> rulesRef = broadcast.value();
        //根据IP地址对应的十进制查找省份名称
        int index = binarySearch(rulesRef, ip);
        String province = "未知";
        if(index != -1) {
            province = rulesRef.get(index)._3();
        }
        return province;
    }
}, DataTypes.StringType);

ArrayList<StructField> structFieldsLog = new ArrayList<>();
structFieldsLog.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
StructType schemaLog = DataTypes.createStructType(structFieldsLog);
Dataset<Row> logDf = spark.createDataFrame(ipLogRDD, schemaLog);
try {
    logDf.createTempView("v_log");
} catch (AnalysisException e) {
    e.printStackTrace();
}

Dataset<Row> result = spark.sql("SELECT ip2Province(ip) province, count(*) counts FROM v_log GROUP BY province ORDER BY counts DESC");
```

### UDF （user defined function）
- UDF    输入一行，返回一个结果    一对一    ip2Province(123123111)   ->  辽宁省
- UDTF   输入一行，返回多行（hive）一对多    spark SQL中没有UDTF，spark中用flatMap即可实现该功能  
- UDAF   输入多行，返回一行 aggregate(聚合) count、sum这些是sparkSQL自带的聚合函数，但是复杂的业务，要自己定义

### DataSet
Dateset是spark1.6以后推出的新的API，也是一个分布式数据集，与RDD相比，保存了跟多的描述信息，概念上等同于关系型数据库中的二维表，基于保存了跟多的描述信息，spark在运行时可以被优化。

Dateset里面对应的的数据是强类型的，并且可以使用功能更加丰富的lambda表达式，弥补了函数式编程的一些缺点，使用起来更方便

在scala中，DataFrame其实就是Dateset[Row]

Dataset的特点：
- 1.一系列分区
- 2.每个切片上会有对应的函数
- 3.依赖关系
- 4.kv类型shuffle也会有分区器
- 5.如果读取hdfs中的数据会感知最优位置
- 6.会优化执行计划
- 7.支持更加智能的数据源


调用Dataset的方法先会生成逻辑计划，然后被spark的优化器进行优化，最终生成物理计划，然后提交到集群中运行！
