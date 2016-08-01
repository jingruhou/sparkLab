package SparkStreaming

import com.cloudera.io.netty.handler.codec.string.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Administrator on 2016/8/1 0001.
  */
object BashSpark {
  case class alert(raw: String)
  def main(args: Array[String]) {

    val confspark = new SparkConf()
      .setAppName("do Application")
    val sc = new SparkContext(confspark)
    val ssc = new StreamingContext(sc, Milliseconds(500))

    // smallest largest
    val kafkaMapParams = Map(
      "zookeeper.connect" -> "",
      "zookeeper.session.timeout.ms" -> "40000",
      "auto.offset.reset" -> "largest",
      "group.id" -> "cb",
      "metadata.broker.list" ->  ""
    )
    val topicsSet = Set("boy")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaMapParams, topicsSet)
    val ok= lines.foreachRDD(rdd => {
      rdd.map(_._2).foreachPartition(
        ub => {
          ub.foreach(b => {
            //业务逻辑
          })
        })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
