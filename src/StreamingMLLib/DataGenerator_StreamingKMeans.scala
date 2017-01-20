package StreamingMLLib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 17-1-19.
  */
object DataGenerator_StreamingKMeans {
  def main(args: Array[String]): Unit = {
    /**
      *  0 环境配置
      */
    val conf = new SparkConf().setAppName("DataGenerator_StreamingKMeans").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      *  1 循环生成数据
      */
    while(true){
      val KMeansStreamingRDD = KMeansDataGenerator.generateKMeansRDD(sc,1,5,3,1.0,1).map(f => Vectors.dense(f))
    }

  }
}
