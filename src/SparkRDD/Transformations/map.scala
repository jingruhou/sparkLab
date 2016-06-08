package SparkRDD.Transformations

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/8.
  */
object map {
  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("map_Transformations").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
      * map 操作 --- map(func)
      *
      * 返回一个新的RDD,由每个元素经过func函数转化后
      */
    val originalRDD = sc.textFile("D:/SogouLab/SogouQ.reduced.tar.gz")
    originalRDD.foreach(println)
  }
}
