package cn.edu360.ser

import java.net.InetAddress

/**
  * Created by zx on 2017/6/25.
  */
//object Rules extends Serializable {
//
//  val rulesMap = Map("hadoop" -> 2.7, "spark" -> 2.2)
//
//  //val hostname = InetAddress.getLocalHost.getHostName
//
//  //println(hostname + "@@@@@@@@@@@@@@@@")
//
//}


//第三种方式，希望Rules在EXecutor中被初始化（不走网络了，就不必实现序列化接口）
object Rules {

  val rulesMap = Map("hadoop" -> 2.7, "spark" -> 2.2)

  val hostname = InetAddress.getLocalHost.getHostName

  println(hostname + "@@@@@@@@@@@@@@@@！！！！")

}
