package SparkML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 17-1-31.
  */
object TrainKMeansModel {
  def main(args: Array[String]): Unit = {
    /**
      * 环境配置
      */
    val conf = new SparkConf().setAppName("TrainKMeansModel").setMaster("local[8]")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)
    /**
      * 加载不同规模的数据 \
      *
      */
    //10万条评分集
    val data_ml100k = sc.textFile("/home/hjr/develop/DataSets/movielens/ml-100k/u.data",8).map(s => Vectors.dense(s.split("\t").map(_.toDouble))).cache()

    //100万条评分集
    val data_ml1m = sc.textFile("/home/hjr/develop/DataSets/movielens/ml-1m/ratings.dat",8).map(s => Vectors.dense(s.split("::").map(_.toDouble))).cache()

    //1000万条评分集
    val data_ml10m100k = sc.textFile("/home/hjr/develop/DataSets/movielens/ml-10M100K/ratings.dat",8).map(s => Vectors.dense(s.split("::").map(_.toDouble))).cache()

    //2000万条评分集
    val data_ml20m = sc.textFile("/home/hjr/develop/DataSets/movielens/ml-20m/ratings.csv",8).map(s => Vectors.dense(s.split(",").map(_.toDouble))).cache()


    val numClusters = 5
    val numIterations = 20
    val initMode = "k-means||"

   /* /**
      * 10万 数据集
      */
    val startTrainTime1 = System.currentTimeMillis()
    //val model = KMeans.train(data_ml100k, numClusters, numIterations)
    val model1 = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(data_ml100k)
    val endTrainTIme1 = System.currentTimeMillis()

    println("训练10万条数据花费时间： "+Seconds(endTrainTIme1-startTrainTime1))
    //println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))

    //println("Vectors 234,1184,2,892079237 is belongs to clusters: "+model1.predict(Vectors.dense("234,1184,2,892079237".split(",").map(_.toDouble))))
    //println("Vectors 389,66,3,880088401 is belongs to clusters: "+model1.predict(Vectors.dense("389,66,3,880088401".split(",").map(_.toDouble))))

    /**
      * 100万 数据集
      */
    val startTrainTime2 = System.currentTimeMillis()
    //val model = KMeans.train(data_ml100k, numClusters, numIterations)
    val model2 = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(data_ml1m)
    val endTrainTIme2 = System.currentTimeMillis()

    println("训练100万条数据花费时间： "+Seconds(endTrainTIme2-startTrainTime2))*/

    /**
      * 1000万 数据集
      */
    val startTrainTime3 = System.currentTimeMillis()
    //val model = KMeans.train(data_ml100k, numClusters, numIterations)
    val model3 = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(data_ml10m100k)
    val endTrainTIme3 = System.currentTimeMillis()

    println("训练1000万条数据花费时间： "+Seconds(endTrainTIme3-startTrainTime3))

   /* /**
      * 2000万 数据集
      */
    val startTrainTime4 = System.currentTimeMillis()
    //val model = KMeans.train(data_ml100k, numClusters, numIterations)
    val model4 = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(data_ml20m)
    val endTrainTIme4 = System.currentTimeMillis()

    println("训练2000万条数据花费时间： "+Seconds(endTrainTIme4-startTrainTime4))*/


    while(true){

    }
  }
}
