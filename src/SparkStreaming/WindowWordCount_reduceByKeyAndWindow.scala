package SparkStreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjr on 17-1-25.
  */
object WindowWordCount_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    //传入的参数为127.0.0.1 8341 30 10
    if(args.length!=4){
      System.err.println("Usage:WindowWordCount <hostname> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("WindowWordCount_reduceByKeyAndWindow").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //创建StreamingContext,batch interval为5秒
    val ssc = new StreamingContext(sc,Seconds(5))
    //val ssc = new StreamingContext(conf,Seconds(5))

    //Socket为数据源
    val lines = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_ONLY_SER)

    val words = lines.flatMap(_.split(" "))

    //window操作，对窗口中的单词进行计数
    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(args(2).toInt), Seconds(args(3).toInt))

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
