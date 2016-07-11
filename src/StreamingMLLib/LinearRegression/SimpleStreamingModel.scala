package StreamingMLLib.LinearRegression

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2016/7/11.
  */
object SimpleStreamingModel {
  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[2]", "hjr_zkjz_First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    val NumFeatures = 100
    val zeroVector = DenseVector.zeros[Double](NumFeatures)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(0.01)

    // create a stream of labeled points
    val labeledStream = stream.map { event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }

    //打印所输入的流
    labeledStream.foreachRDD(line =>line.collect().foreach(println))

    // train and test model on the stream, and print predictions for illustrative purposes
    model.trainOn(labeledStream)
    //打印每一个流的预测值

    //model.predictOn(labeledStream).print()


    //model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    ssc.start()
    ssc.awaitTermination()

  }
}
