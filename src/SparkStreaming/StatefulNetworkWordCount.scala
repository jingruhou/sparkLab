package SparkStreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * Created by hjr on 17-1-24.
  */
/**
  * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
  * second starting with initial value of word count.
  * Usage: StatefulNetworkWordCount <hostname> <port>
  *   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
  *   data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * and then run the example
  *    `$ bin/run-example
  *      org.apache.spark.examples.streaming.StatefulNetworkWordCount localhost 9999`
  */
object StatefulNetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StatefulNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    //函数字面量，输入的当前值与前一次的状态结果进行累加
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum

      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    //输入类型为K,V,S,返回值类型为K,S
    //V对应为带求和的值，S为前一次的状态
    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[4]")

    //每一秒处理一次
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //当前目录为checkpoint结果目录，后面会讲checkpoint在Spark Streaming中的应用
    ssc.checkpoint("Resources/Streaming/StatefulNetworkWordCount")

    //RDD的初始化结果
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))


    //使用Socket作为输入源，本例ip为localhost，端口为9999
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    //flatMap操作
    val words = lines.flatMap(_.split(" "))
    //map操作
    val wordDstream = words.map(x => (x, 1))

    //updateStateByKey函数使用
    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner (ssc.sparkContext.defaultParallelism), true, initialRDD)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
