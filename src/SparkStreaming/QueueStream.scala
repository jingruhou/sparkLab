package SparkStreaming

import org.apache.spark.examples.streaming.StreamingExamples

import scala.collection.mutable.Queue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2016/6/30.
  *
  * 队列流数据处理
  */
object QueueStream {
  def main(args: Array[String]) {

    /**
      *  0 参数设置
      *
      * 设置输出日志级别
      * 实例化SparkConf
      * 实例化StreamingContext
      */
    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[4]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    /**
      *  1 创建一个RDD队列
      *  使得RDD能够push到输入流里面去
      */
    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Int]]()

    /**
      *  2 创建一个QueueInputDStream，并且做一些处理（map/reduce...）
      */
    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //打印结果
    reducedStream.print()
    //启动计算
    ssc.start()

    /**
      * 3 创建一些RDD，并且push到rddQueue（rdd队列）
      */
    // Create and push some RDDs into rddQueue
    for (i <- 1 to 30) {
      // 同步队列
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(1000)//线程休眠1秒钟
    }
    //通过程序停止StreamingContext的运行
    ssc.stop()
  }
}
