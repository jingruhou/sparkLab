package SparkML

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
// $example off$
import org.apache.spark.sql.{DataFrame, SQLContext}
/**
  * Created by Administrator on 2016/6/13.
  */
object KMeansExample {
  def main(args: Array[String]): Unit = {
    // 0 创建SparkContext 和 SQL Context
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 1 创建DataFrame
    val dataset: DataFrame = sqlContext.createDataFrame(
      Seq((1, Vectors.dense(0.0, 0.0, 0.0)),
          (2, Vectors.dense(0.1, 0.1, 0.1)),
          (3, Vectors.dense(0.2, 0.2, 0.2)),
          (4, Vectors.dense(9.0, 9.0, 9.0)),
          (5, Vectors.dense(9.1, 9.1, 9.1)),
          (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")

    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val model = kmeans.fit(dataset)

    // Shows the result
    println("Final Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    sc.stop()
  }
}
