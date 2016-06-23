package SparkMLlib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Administrator on 2016/6/23.
  */
object StreamingLogisticRegression {
  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println(
        "Usage: StreamingLogisticRegression <trainingDir> <testDir> <batchDuration> <numFeatures>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("StreamingLogisticRegression")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    val trainingData = ssc.textFileStream(args(0)).map(LabeledPoint.parse)
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

    val model = new StreamingLogisticRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(args(3).toInt))

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
