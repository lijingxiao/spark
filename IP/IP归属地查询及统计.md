需求：根据访问日志的ip地址计算出访问者的归属地，并且按照省份统计访问次数，然后将计算结果写入mysql。

- 整理数据，切分出ip字段，然后将ip地址转换成十进制
- 加载规则，整理规则，取出有用字段，将数据缓存到内存中（Executor端）
- 将访问log与ip规则进行匹配（二分查找）
- 取出对应的省份名称将其与一组合在一起
- 按照省份尽心聚合
- 将聚合后的数据写入mysql

假如ip规则文件在Driver所在的机器上：
- 在driver端获取全部的IP规则到内存中
- 调用sc上的广播方法，广播变量的引用（还在Driver端），注意broadcast是同步的，如果没有广播完成，就不会进行下一步操作
- 内容一旦广播出去，就不能再改变，如果需要实时改变的规则，可以将规则放到Redis

序列化：
存在闭包（函数内部引用外部变量或实例）的情况下需要实例化。
在Driver端实例化一个对象，序列化之后发送到Executor端，Executor反序列化之后会生成一个新的实例，两者不在同一个JVM中。
- 在Driver端定义一个类，并new一个实例，在RDD的函数中使用了该实例，那么伴随着Task的发送，该实例会发送到Executor，**每个Task中都会有一份单独的实例**。
- 在Driver端初始化一个Object，在RDD的函数中引用了该Object，那么该单利对象会伴随着Task发送出去，**每个Execuotr进程中只有一份**。因为Object是静态的，单例的，每个进程只有一份。
- 静态代码块（static/Object）在哪里引用，就在哪里执行，直接在函数内部引用定义的Object，也就是在Executor中进行初始化，一个进程中初始化一次。


foreach、forechaPartition是Action，会触发任务提交（**Action的特点是会触发任务，runJob，有数据计算出的结果产生**）
collect、take、count（收集到Driver端的Action）
