package SparkMLlib


import org.apache.spark.SparkConf
// $example on$
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
// $example off$
import org.apache.spark.streaming._
/**
  * Created by Administrator on 2016/6/23.
  */
object StreamingLinearRegressionExample {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: StreamingLinearRegressionExample <trainingDir> <testDir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("StreamingLinearRegressionExample")
    val ssc = new StreamingContext(conf, Seconds(1))

    // $example on$
    val trainingData = ssc.textFileStream(args(0)).map(LabeledPoint.parse).cache()
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

    val numFeatures = 3
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
    // $example off$

    ssc.stop()
  }
}
