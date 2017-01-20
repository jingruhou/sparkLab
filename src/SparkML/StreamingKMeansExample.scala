package SparkML

import org.apache.spark.SparkConf
// $example on$
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Administrator on 2016/6/22.
  */
object StreamingKMeansExample {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: StreamingKMeansExample " +
          "<trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>")
      System.exit(1)
    }

    // $example on$
    val conf = new SparkConf().setAppName("StreamingKMeansExample").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    //val KMeansStreamingRDD = KMeansDataGenerator.generateKMeansRDD(sc,1,5,3,1.0,1).map(f => Vectors.dense(f))

    val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      .setRandomCenters(args(4).toInt, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
    // $example off$
  }
}
