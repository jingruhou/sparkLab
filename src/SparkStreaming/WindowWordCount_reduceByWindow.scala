package SparkStreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjr on 17-1-25.
  */
object WindowWordCount_reduceByWindow {
  def main(args: Array[String]): Unit = {
    if(args.length!=4){
      System.err.println("Usage:WindowWordCount <hostname> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("WindowWordCount_countByWindow").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    //定义checkpoint目录为当前目录
    ssc.checkpoint("Resources/Streaming/WindowWordCount_countByWindow")

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))

    //reduceByWindow方法基于滑动窗口对源DStream中的元素进行聚合操作，返回包含单元素的一个新的DStream
    val reduceByWindow=words.map(x=>1).reduceByWindow(_+_,_-_,Seconds(args(2).toInt), Seconds(args(3).toInt))

    reduceByWindow.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
