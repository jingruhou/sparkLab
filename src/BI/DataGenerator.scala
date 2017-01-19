package BI

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hjr on 17-1-19.
  */
object DataGenerator {

  def main(args: Array[String]): Unit = {
    /**
      * （0）环境配置
      */
    //val conf = new SparkConf().setMaster("spark://hjr:7077").setAppName("KMeansDataGenerator")
    val conf = new SparkConf().setMaster("local[*]").setAppName("KMeansDataGenerator")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)//设置日志输出级别

    /**
      * （1）一次性生成批量的数据
      */
    val KMeansRDD = KMeansDataGenerator.generateKMeansRDD(sc,500,5,3,1.0,2)

    KMeansRDD.count()
    /**
      * KMeansDataGenerator.generateKMeansRDD 返回的数据类型为：RDD[Array[Double]
      *
      * 查看KMeansModel模型训练源码：
      * def run(data: RDD[Vector]): KMeansModel
      *
      * 要求输入的数据格式为RDD[Vector]向量形式的RDD，所以需要数据格式的转换，通过map操作来实现
      *
      * 将generateKMeansRDD生成的数组型的RDD转化成向量形式的RDD：map(f => Vectors.dense(f)),
      *
      * 需要导入的包为：org.apache.spark.mllib.linalg.Vectors
      */
    KMeansRDD.map(f => Vectors.dense(f)).foreach(println)
    /**
      *（2）循环生成数据
      *
      * Question：这种方式只是陷入了死循环，并没有循环执行，卡在那里了
      *
      * 需要继续想办法......
      */
    while(true){
      val KMeansStreamingRDD = KMeansDataGenerator.generateKMeansRDD(sc,1,5,3,1.0,2)
      KMeansStreamingRDD.saveAsTextFile("hjrTemp")
    }
    /**
      * （3）聚类模型训练数据
      */
    val GenerateKMeansRDD = KMeansDataGenerator.generateKMeansRDD(sc,50,2,2,1.0,2)
      .map(f => Vectors.dense(f))

    /**
      * 数据格式
      *
      * Array[Array[Double]] =Array(
      * Array(2.2838106309461095, 1.8388158979655758, -1.8997332737817918),
      * Array(-0.6536454069660477, 0.9840269254342955, 0.19763938858718594),
      * Array(0.24415182644986977, -0.4593305783720648, 0.3286249752173309),
      * Array(1.8793621718715983, 1.4433606519575122, -0.9420612755690412),
      * Array(2.7663276890005077, -1.4673057796056233, 0.39691668230812227),
      * Array(2.8920306508512708, 1.157869653944906, -1.3700753767081342),
      * ......)
      */

    val initMode = "k-means||"
    val numClusters = 2
    val numIterations = 20
    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(GenerateKMeansRDD)

    //误差计算
    val WSSSE = model.computeCost(GenerateKMeansRDD)
    println("Within Set Sum of Squared Errors = "+WSSSE)

    //保存模型
    val ModelPath = "out/hjrTemp"
    model.save(sc,ModelPath)

    val sameModel = KMeansModel.load(sc,ModelPath)

    sc.stop()
  }
}
