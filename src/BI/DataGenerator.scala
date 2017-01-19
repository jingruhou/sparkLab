package BI

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.KMeansDataGenerator

/**
  * Created by hjr on 17-1-19.
  */
object DataGenerator {

  def main(args: Array[String]): Unit = {
    /**
      * 环境配置
      */
    val conf = new SparkConf().setMaster("local").setAppName("KMeansDataGenerator")
    val sc = new SparkContext(conf)
    /**
      * 数据生成
      */
    val KMeansRDD = KMeansDataGenerator.generateKMeansRDD(sc,50,5,3,1.0,2)

    KMeansRDD.count()

    KMeansRDD.take(50)

    sc.stop()
  }
}
