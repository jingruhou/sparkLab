package SparkRDD.Transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 17-2-11.
  */
object rdd0 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("RDD")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(1 to 9)
    val b = a.map(x => x*2)

    b.collect()
  }
}
