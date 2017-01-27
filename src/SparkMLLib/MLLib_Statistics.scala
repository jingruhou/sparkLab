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
      * 注意：
      *
      * 在这个编辑器里面的“/t”不是标准的分隔符，识别不了
      * 所以这里我换成了“ ”空格分隔符
      *
      */
    val data_path="Resources/data/Statistics/sample_stat.txt"//本地数据文件路径

    /**
      * 打印原始数据
      */
    sc.textFile(data_path).foreach(println)
    /**
      * （1）使用textFile("数据文件路径")加载数据
      * （2）使用map(“分隔符”)操作分割/映射每一行的数据
      * （3）使用map("自定义函数")操作
      * （4）map("自定义函数")：（输入为每一行中的每一列数据）
      *       {
      *         f => f.map(f => f.toDouble)
      *       }
      *       每一行的数据通过分隔符映射出来以后，
      *       对每一个数据，再次做map映射，
      *       这次的map映射中的自定义函数为：（输入为每一列数据）
      *       {
      *         f => f.toDouble
      *       }
      *       对每一个映射出来的数据进行格式转换，转化为Double型数据
      */

    val data = sc.textFile(data_path).map(_.split(" ")).map(f => f.map(f => f.toDouble))

    /**
      * 继续map操作
      * {
      *   f => Vectors.dense(f)
      * }
      * map("自定义函数")：
      *
      * 输入为map的每一行数据
      *
      * 输出为一个密度向量（由一个Double型的数组转化而来）
      *
      * Creates a dense vector from a double array
      *
      */
    val data1 = data.map(f => Vectors.dense(f))

    data1.collect().foreach(println)
    /**
      * Computes column-wise summary statistics for the input RDD[Vector]
      *
      */
    val stat1 = Statistics.colStats(data1)

    /**
      * Maximum value of each column.
      */
    println("每一列的最大值： "+stat1.max)

    /**
      * Minimum value of each column.
      */
    println("每一列的最小值： "+stat1.min)

    /**
      * Sample mean vector.
      */
    println("每一列的平均值： "+stat1.mean)

    /**
      * Sample variance vector. Should return a zero vector if the sample size is 1.
      */
    println("每一列的方差值： "+stat1.variance)

    /**
      * L1 norm of each column
      */
    println("L1范数： "+stat1.normL1)

    /**
      * Euclidean magnitude of each column
      */
    println("L2范数： "+stat1.normL2)
  }

}
