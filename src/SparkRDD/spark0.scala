package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2016/6/7.
  */
object spark0 {
  def main(args:Array[String]): Unit ={
    //初始化配置
    val conf = new SparkConf().setAppName("Spark_hjr_rdd0").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val outclinical_diago_rdd = sc.textFile("D:/SogouLab/SogouQ3.txt")
    outclinical_diago_rdd.foreach(println)
  }
}
