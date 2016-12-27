package SparkMLLib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 16-12-27.
  */
object MLLib_Statistics {

  def main(args: Array[String]): Unit = {
    /**
      * 环境设置
      */
    val conf = new SparkConf().setAppName("MLLib_Statistics").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * 读取数据，转换成RDD[Vector]类型
      *
      */
    val data_path="Resources/data/Statistics/sample_stat.txt"

    val data = sc.textFile(data_path).map(_.split(" ")).map(f => f.map(f => f.toDouble))

    val data1 = data.map(f => Vectors.dense(f))

    val stat1 = Statistics.colStats(data1)
    stat1.max
    stat1.min
    stat1.mean
    stat1.variance
    stat1.normL1
    stat1.normL2
  }

}
