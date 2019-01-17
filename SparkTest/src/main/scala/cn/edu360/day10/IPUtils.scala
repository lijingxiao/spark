package cn.edu360.day10

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

/**
  * Created by zx on 2017/10/20.
  */
object IPUtils {

  def broadcastIpRules(ssc: StreamingContext, ipRulesPath: String): Broadcast[Array[(Long, Long, String)]] = {
    //现获取sparkContext
    val sc = ssc.sparkContext
    val rulesLines:RDD[String] = sc.textFile(ipRulesPath)
    //整理ip规则数据
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    //将分散在多个Executor中的部分IP规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()


    //将Driver端的数据广播到Executor
    //广播变量的引用（还在Driver端）
    sc.broadcast(rulesInDriver)
  }
}
