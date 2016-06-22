package SparkMLlib

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Administrator on 2016/6/22.
  */
object StreamingKMeans {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("SparkStreaming_KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val trainingData = ssc.textFileStream("D:/streamingData/kmeans/trainingDir/0.txt").map(Vectors.parse)
    val testData = ssc.textFileStream("D:/streamingData/kmeans/testDir/1.txt").map(LabeledPoint.parse)

    val numDimensions = 3
    val numClusters = 2
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions,0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
