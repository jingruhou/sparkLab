package SparkMLLib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 17-1-26.
  */
object als {
  def main(args: Array[String]): Unit = {
    /**
      * 1 构建Spark对象
      */
    val conf = new SparkConf().setAppName("ALS").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("ALS").setMaster("spark://hjr:7077")

    val sc = new SparkContext(conf)
    //设置日志输出级别
    Logger.getRootLogger.setLevel(Level.WARN)

    /**
      * 2 读取样本数据
      */
    //val data = sc.textFile("Resources/MLLib/als/test.data")
    val data = sc.textFile("Resources/MLLib/als/ratings.dat")
    //val data = sc.textFile("/sparkSourceData/als/test.data")

    val ratings = data.map(_.split("::") match{
      case Array(user,item,rate,ts) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()
    //查看第一条记录
    println(ratings.first())

   /**
      * 3 建立模型
      */
    val rank = 10
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    /**
      * 4 预测结果
      */
    val usersProducts = ratings.map{
      case Rating(user, product, rate) =>
        (user, product)
    }

    val predictions =
      model.predict(usersProducts).map{
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }

    val ratesAndPreds = ratings.map{
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map{
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
    /**
      * 5 保存/加载模型
      */
    val ModelPath = "Resources/MLLib/als/ALS_Model"
    //val ModelPath = "/sparkSourceData/als/hjrTemp/ALS_Model"
    model.save(sc, ModelPath)
    val sameModel = MatrixFactorizationModel.load(sc, ModelPath)

  }
}
