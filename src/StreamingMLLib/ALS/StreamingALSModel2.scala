package StreamingMLLib.ALS

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjr on 17-2-10.
  */
object StreamingALSModel2 {
  def main(args: Array[String]): Unit = {
    /**
      * 0 实例化运行环境---构建Spark对象
      */
    val conf = new SparkConf().setAppName("StreamingALSModel2").setMaster("local[8]")
    val ssc = new StreamingContext(conf,Seconds(1))
    //设置日志输出级别
    Logger.getRootLogger.setLevel(Level.WARN)

    /**
      * 1 接收数据
      */
    val lines = ssc.socketTextStream("127.0.0.1", 8342, StorageLevel.MEMORY_AND_DISK)

    //println("时间："+System.currentTimeMillis()+" 接收的数据为： "+lines.print())

    /**
      * 测试
      */
    //val words = lines.flatMap(_.split("::"))
    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //wordCounts.print()


    val ratings = lines.map(_.split("::") match{
      case Array(user,item,rate,ts) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()
    //查看接受到的数据记录
    ratings.print()
    //ratings.foreachRDD(data => data.collect().foreach(println))
    /**
      * 2 构建ALS模型
      * 设置默认参数
      */

    /**
      *  3 创建一个labeled points流
      *  create a stream of labeled points
      */

    /**
      * 4 打印所输入的流
      */


    /**
      * 开启StreamingContext
      * Start the execution of the streams
      */
    ssc.start()

    /**
      * 等待执行，直到结束
      * Wait for the execution to stop.
      * Any exceptions that occurs during the execution will be thrown in this thread.
      */
    ssc.awaitTermination()
  }
}
