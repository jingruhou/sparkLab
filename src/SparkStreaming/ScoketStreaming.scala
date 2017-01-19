package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjr on 17-1-19.
  */
object ScoketStreaming {
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[24]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf,Seconds(1))

    val lines = sc.socketTextStream("hjr",9998)
    val words = lines.flatMap(_.split((" ")))
    val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
    wordCounts.print()
    sc.start()
    sc.awaitTermination()
  }
}
