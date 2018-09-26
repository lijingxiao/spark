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


foreach、forechaPartition是Action，会触发任务提交（**Action的特点是会触发任务，runJob，有数据计算出的结果产生**）
collect、take、count（收集到Driver端的Action）
