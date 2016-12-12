package SparkRDD.Transformations

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 2016/12/12.
  */
object flatMap {
  def main(args:Array[String]): Unit ={
    /**
      * 环境配置
      */
    val conf = new SparkConf().setMaster("local").setAppName("flatMap_Transformations")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    /**
      * flatMap实现的功能：对每个输入的元素生成多个输出元素
      *
      * 返回值：返回的不是一个元素，而是一个返回值序列的迭代器
      *
      * 输出的RDD倒不是由迭代器器组成的，我们得到的是一个包含各个迭代器可访问的所有元素的RDD
      *
      * flatMap()的一个简单用途是把输入的字符串切分为单词
      */
    val lines = sc.parallelize(List("hello spark","hadoop"))
    val words = lines.flatMap(line => line.split(" "))
    println(words.first())

    words.foreach(println)
  }
}
