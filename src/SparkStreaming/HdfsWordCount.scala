package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Administrator on 2016/6/14.
  */
object HdfsWordCount {
  def main(args: Array[String]) {
    /*if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }*/
    //设置输出日志级别
    StreamingExamples.setStreamingLogLevels()
    //创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]")
    // Create the context
    //创建StreamingContext对象，与集群进行交互
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    //如果目录中有新创建的文件，则读取
    //val lines = ssc.textFileStream(args(0))
    val lines = ssc.textFileStream("Resources/Streaming/HdfsWordCount/")

    //将每一行数据分割为单词（分隔符为空格）
    val words = lines.flatMap(_.split(" "))
    //统计单词出现的次数
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //打印结果
    wordCounts.print()
    //启动Spark Streaming
    ssc.start()
    //一直运行，除非人为干预再停止
    ssc.awaitTermination()
  }

}
