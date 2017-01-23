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
      *
      *  具体步骤如下：
      *
      *  1）随机生成一条KMeansStreamingRDD的数据（数据的簇数量、维度等参数自行设定，或根据已有模型设定）
      *
      *  2）初始化StreamingKMeans模型
      *
      *  3）训练模型
      *
      *  4）预测数据
      *
      *  5）
      *
      *  6）问题：这个就不是StreamingKMeans在线计算了，而是死循环了
      */
    while(true){

      val KMeansStreamingRDD = KMeansDataGenerator.generateKMeansRDD(sc,1,5,3,1.0,1).map(f => Vectors.dense(f))
    }

  }
}
