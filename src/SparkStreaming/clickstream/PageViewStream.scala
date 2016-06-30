package SparkStreaming.clickstream

import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Administrator on 2016/6/30.
  *
  * 流式数据处理过程
  */
object PageViewStream {
  def main(args: Array[String]) {
    // 0 输入参数判断： 度量参数 主机地址 主机端口（eg:errorRatePerZipCode localhost 44444）
    if (args.length != 3) {
      System.err.println("Usage: PageViewStream <metric> <host> <port>")
      System.err.println("<metric> must be one of pageCounts, slidingPageCounts," +
        " errorRatePerZipCode, activeUserCount, popularUsersSeen")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()//设置日志级别
    // 1 属性设置
    val metric = args(0)//度量参数
    val host = args(1)//主机地址
    val port = args(2).toInt//端口号

    /*StreamingContext-----------------------------------------------------------------------------
    Create a StreamingContext by providing the details necessary for creating a new SparkContext.
    @param master cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
    @param appName a name for your job, to display on the cluster web UI
    @param batchDuration the time interval at which streaming data will be divided into batches
    ----------------------------------------------------------------------------------------------*/
    // 2 创建StreamingContext(master,appName,batchDuration)
    // Create the context
    val ssc = new StreamingContext("local[2]", "PageViewStream", Seconds(1),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass).toSeq)

    // 3 在目标主机：端口上创建一个ReceiverInputDStream，将每一行数据转化为一个PageView
    // Create a ReceiverInputDStream on target host:port and convert each line to a PageView
    val pageViews = ssc.socketTextStream(host, port)
      .flatMap(_.split("\n"))
      .map(PageView.fromString(_))

    // 4-1 返回每一个batch中：每一个url页面浏览的次数---countByValue
    // Return a count of views per URL seen in each batch
    val pageCounts = pageViews.map(view => view.url).countByValue()

    // 4-2 返回最近10秒钟：每一个url页面在一个滑动窗口的页面浏览次数
    // Return a sliding window of page views per URL in the last ten seconds
    val slidingPageCounts = pageViews.map(view => view.url)
      .countByValueAndWindow(Seconds(10), Seconds(2))

    // 4-3 返回最近30秒钟：每一个zip code 页面错误率
    // Return the rate of error pages (a non 200 status) in each zip code over the last 30 seconds
    val statusesPerZipCode = pageViews.window(Seconds(30), Seconds(2))
      .map(view => ((view.zipCode, view.status)))
      .groupByKey()
    val errorRatePerZipCode = statusesPerZipCode.map{
      case(zip, statuses) =>
        val normalCount = statuses.count(_ == 200)
        val errorCount = statuses.size - normalCount
        val errorRatio = errorCount.toFloat / statuses.size
        if (errorRatio > 0.05) {
          "%s: **%s**".format(zip, errorRatio)
        } else {
          "%s: %s".format(zip, errorRatio)
        }
    }

    // 4-4 返回最近15秒钟：唯一的用户量
    // Return the number unique users in last 15 seconds
    val activeUserCount = pageViews.window(Seconds(15), Seconds(2))
      .map(view => (view.userID, 1))
      .groupByKey()
      .count()
      .map("Unique active users: " + _)

    // 用户列表
    // An external dataset we want to join to this stream
    val userList = ssc.sparkContext.parallelize(Seq(
      1 -> "Patrick Wendell",
      2 -> "Reynold Xin",
      3 -> "Matei Zaharia"))

    // 度量参数匹配
    metric match {
      case "pageCounts" => pageCounts.print()
      case "slidingPageCounts" => slidingPageCounts.print()
      case "errorRatePerZipCode" => errorRatePerZipCode.print()
      case "activeUserCount" => activeUserCount.print()
      case "popularUsersSeen" =>
        // Look for users in our existing dataset and print it out if we have a match
        pageViews.map(view => (view.userID, 1))
          .foreachRDD((rdd, time) => rdd.join(userList)
            .map(_._2._2)
            .take(10)
            .foreach(u => println("Saw user %s at time %s".format(u, time))))
      case _ => println("Invalid metric entered: " + metric)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
