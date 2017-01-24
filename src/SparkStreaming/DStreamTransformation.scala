package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjr on 17-1-24.
  */
object DStreamTransformation {
  def main(args: Array[String]): Unit = {
    /**
      * 环境配置
      */
    val conf = new SparkConf().setAppName("DStreamTransformation").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(10))

    //读取本地文件------Resources/Streaming/DStreamTransformation文件夹
    val lines = ssc.textFileStream("Resources/Streaming/DStreamTransformation")

    //将读取的每一行数据按照空格分割
    //flatMap()输入为lines-每一行数据，输出为包含分割后的每行所有的单词
    val words = lines.flatMap(_.split(" "))

    //对每一个出现的单词进行MAP映射，映射为（word，1）键值对形式
    val wordMap = words.map(x => (x, 1))
    //词频统计
    val wordCounts = wordMap.reduceByKey(_ + _)

    //对词频统计结果进行过滤（过滤词频次数不大于1的单词）
    val filteredWordCounts = wordCounts.filter(_._2>1)
    //对过滤后的词频统计结果进行计数
    val numOfCount = filteredWordCounts.count()

    //元素类型为k的DStream，返回的是（K，Long）键值对形式的DStream
    //Long对应的值为源DStream中各个RDD的key出现的次数
    //也就是说，这个返回的结果是每一个单词出现的次数(是否包含重复单词？？？)
    val countByValue = words.countByValue()

    //两个DStream合并
    //val union = words.union(word1)

    //通过RDD-to-RDD函数作用于源码DStream中的各个RDD，返回的结果是一个新的RDD
    val transform = words.transform(x => x.map((x => (x, 1))))

    /**
      * 打印输出结果
      */

    //显示原文件
    lines.print()

    //打印flatMap结果
    words.print()

    //打印map结果
    wordMap.print()

    //打印reduceByKey结果
    wordCounts.print()

    //打印filter结果
    filteredWordCounts.print()

    //打印count结果
    numOfCount.print()

    //打印countByValue结果
    countByValue.print()

    //打印union结果
    //union.print()

    //打印transform结果
    transform.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
