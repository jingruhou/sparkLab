package SparkMLLib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/11.
  */
object MLLib {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMLLib_Basic").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * 创建 稠密向量 <1.0,2.0,3.0>
      * Vectors.dense接收一串值或一个数组
      *
      */
    val denseVec1 = Vectors.dense(1.0,2.0,3.0)
    val denseVec2 = Vectors.dense(Array(1.0,2.0,3.0))

    /**
      * 创建 稀疏向量 <1.0,0.0,2.0,0.0>
      * 向量的维度（4） 以及非零位的位置和对应的值
      */
    val sparseVec1 = Vectors.sparse(4,Array(0,2),Array(1.0,2.0))




  }
}
