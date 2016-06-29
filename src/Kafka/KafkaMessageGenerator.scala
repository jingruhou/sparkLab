package Kafka

import java.util.{Properties, Random, UUID}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.codehaus.jettison.json.JSONObject
/**
  * Created by Administrator on 2016/6/27.
  */
object KafkaMessageGenerator {
  private val random = new Random()
  private var pointer = -1
  private val os_type = Array(
    "Android", "IPhone OS",
    "None", "Windows Phone")

  def click() : Double = {
    random.nextInt(10)
  }

  def getOsType() : String = {
    pointer = pointer + 1
    if(pointer >= os_type.length) {
      pointer = 0
      os_type(pointer)
    } else {
      os_type(pointer)
    }
  }

  def main(args: Array[String]): Unit = {
    //val topic = "user_events"
    val topic = "ZKJZ0"
    //本地虚拟机ZK地址
    //val brokers = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
    val brokers = "zkjz0:9092,zkjz1:9092,zkjz2:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    while(true) {
      // prepare event data
      val event = new JSONObject()
      event
        .put("uid", UUID.randomUUID())//随机生成用户id
        .put("event_time", System.currentTimeMillis.toString) //记录时间发生时间
        .put("os_type", getOsType) //设备类型
        .put("click_count", click) //点击次数

      // produce event message
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)

      Thread.sleep(1000)
    }
  }
}
