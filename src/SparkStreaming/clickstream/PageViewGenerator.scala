package SparkStreaming.clickstream

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random

/**
  * Created by Administrator on 2016/6/30.
  *
  * 点击流---数据生成器
  */

/** Represents a page view on a website with associated dimension data. */
/**  定义一个PageView类：展示一个关联各个维度数据的网站PV( 网站页面浏览量 )
  *
  * url 页面地址
  * status 页面状态
  * zipCode
  * userID 用户ID
  */
class PageView(val url: String, val status: Int, val zipCode: Int, val userID: Int)
  extends Serializable {
  override def toString(): String = {
    "%s\t%s\t%s\t%s\n".format(url, status, zipCode, userID)
  }
}
/**
  * 定义一个PageView对象
  *
  * 对象里面定义一个fromString（in:String）方法
  */
object PageView extends Serializable {
  def fromString(in: String): PageView = {
    val parts = in.split("\t")
    new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
  }
}
/**
  * Generates streaming events to simulate page views on a website.
  * 生成 事件流 来 模拟 网站页面浏览量
  *
  * This should be used in tandem with PageViewStream.scala. Example:
  *
  * 运行 事件流 生成器
  * To run the generator
  * `$ bin/run-example \
  *    org.apache.spark.examples.streaming.clickstream.PageViewGenerator 44444 10`
  *
  * 运行 处理生成器生成的事件流 的进程
  * To process the generated stream
  * `$ bin/run-example \
  *    org.apache.spark.examples.streaming.clickstream.PageViewStream errorRatePerZipCode localhost 44444`
  *
  */
// scalastyle:on
/**
  * 定义 事件流 生成器 对象
  */
object PageViewGenerator {
  // 定义页面，并且设置页面的权重值
  val pages = Map("http://foo.com/" -> .7,
    "http://foo.com/news" -> 0.2,
    "http://foo.com/contact" -> .1)
  // 定义http响应状态
  val httpStatus = Map(200 -> .95,
    404 -> .05)
  // 定义用户zipCode
  val userZipCode = Map(94709 -> .5,
    94117 -> .5)
  // 定义用户ID
  val userID = Map((1 to 100).map(_ -> .01): _*)

  //
  def pickFromDistribution[T](inputMap: Map[T, Double]): T = {
    val rand = new Random().nextDouble()//生成随机数据
    var total = 0.0
    for ((item, prob) <- inputMap) {
      total = total + prob
      if (total > rand) {
        return item
      }
    }
    inputMap.take(1).head._1 // Shouldn't get here if probabilities add up to 1.0
  }
  // 获取点击事件
  def getNextClickEvent(): String = {
    val id = pickFromDistribution(userID)
    val page = pickFromDistribution(pages)
    val status = pickFromDistribution(httpStatus)
    val zipCode = pickFromDistribution(userZipCode)
    new PageView(page, status, zipCode, id).toString()
  }
  // 主类：入口---开始业务逻辑
  def main(args: Array[String]) {
    // 0 输入参数判断：端口、每秒页面访问次数
    if (args.length != 2) {
      System.err.println("Usage: PageViewGenerator <port> <viewsPerSecond>")
      System.exit(1)
    }
    // 1 属性设置
    val port = args(0).toInt//端口转化为Int型
    val viewsPerSecond = args(1).toFloat//每秒页面访问次数转化为float型
    val sleepDelayMs = (1000.0 / viewsPerSecond).toInt//
    val listener = new ServerSocket(port)//实例化SocketServer
    println("（监听端口）Listening on port: " + port)

    // 2 开始输出流数据
    while (true) {
      val socket = listener.accept()//开始监听连接（直到监听到有连接，程序才往下走/阻塞在这里的，直到有连接）
      // 如果有连接，就新建一个线程，来分配任务
      new Thread() {
        override def run(): Unit = {
          //打印连接的客户端信息
          println("Got client connected from: " + socket.getInetAddress)
          // 从已有的输出流OutputStream实例化一个PrintWriter输出流
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(sleepDelayMs)//线程休眠
            out.write(getNextClickEvent())//将 生成器 生成的 点击事件流 写入到 输出流out 里面
            out.flush()//输出流数据
          }
          socket.close()//关闭Socket
        }
      }.start()//开启线程
    }
  }
}
