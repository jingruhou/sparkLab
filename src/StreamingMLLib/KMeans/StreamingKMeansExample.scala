package StreamingMLLib.KMeans

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2016/6/14.
  */
object StreamingKMeansExample {
  /**
    * 数据输入路径
    *
    * @param args
    */
  def main(args: Array[String]) {

    // 0 输入参数判断
    if (args.length != 5) {
      System.err.println(
        "Usage: StreamingKMeansExample " +
          "<trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>")
      System.exit(1)
    }

    // 1-1 初始化SparkConf（设置master、appname、jar等）
    val conf = new SparkConf().setMaster("local").setAppName("StreamingKMeansExample1")
    // 1-2 使用SparkConf初始化StreamingContext
    val ssc = new StreamingContext(conf,Seconds(args(2).toLong))

    // 2-1 加载训练数据路径---将数据转化为Vectors向量形式
    val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)
    // 2-2 加载测试数据路径---将数据转化为LabeledPoint形式
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

    /*
     * $ bin/run-example mllib.StreamingKMeansExample trainingDir testDir 5 3 2
     */
    // 3 初始化StreamingKMeans模型（K值//设置DecayFactor **系数//初始中心点的个数）
    val model = new StreamingKMeans()
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      .setRandomCenters(args(4).toInt,0.0)

    // 4 训练模型
    model.trainOn(trainingData)

    // 5 预测
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    // 6 启动 流式处理
    ssc.start()
    ssc.awaitTermination()
  }
}
