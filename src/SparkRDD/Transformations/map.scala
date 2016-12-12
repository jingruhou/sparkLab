package SparkRDD.Transformations

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/8.
  */
object map {
  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("map_Transformations").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * map 操作 --- map(func)
      *
      * 返回一个新的RDD,由每个元素经过func函数转化后
      */
    val originalRDD = sc.textFile("Resources/data/Sogou/SogouQ1.txt")
    originalRDD.foreach(println)

    // 0
    val result0 = originalRDD.flatMap(line =>line.split("\t")).map(word =>(word,1)).reduceByKey(_ + _)
    result0.foreach(println)

    /**
      * 2016-12-12 hjr
      *
      * 计算RDD中各值的平方（map算子应用）
      */
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println("mkString(',')使用： "+result.collect().mkString(","))//将collect集合里面的元素构造成字符串打印出来

    /**
      * mkString方法的定义
      *
      * def mkString(start: String, sep: String, end: String): String =
      * addString(new StringBuilder(), start, sep, end).toString
      *
      * def mkString(sep: String): String = mkString("", sep, "")
      *
      * def mkString: String = mkString("")
      *
      */
    println("mkString使用： "+result.collect().mkString)
    println("mkString('start-','*','-end')使用： "+result.collect().mkString("start-","*","-end"))
  }
}
