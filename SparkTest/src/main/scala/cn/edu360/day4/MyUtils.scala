package cn.edu360.day4

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.streaming.StreamingContext

import scala.io.{BufferedSource, Source}

/**
  * Created by zx on 2017/10/9.
  */
object MyUtils {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def data2MySQL(it: Iterator[(String, Int)]): Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //先获得一个JDBC连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123568")
    //将数据通过Connection写入到数据库
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")
    //将分区中的数据一条一条写入到MySQL中
    it.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    //将分区中的数据全部写完之后，在关闭连接
    if(pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {
    //数据是在内存中
    val rules: Array[(Long, Long, String)] = readRules("/Users/zx/Desktop/ip/ip.txt")
    //将ip地址转换成十进制
    val ipNum = ip2Long("114.215.43.42")
    //查找
    val index = binarySearch(rules, ipNum)
    //根据脚本到rules中查找对应的数据
    val tp = rules(index)
    val province = tp._3
    println(province)

  }
}
