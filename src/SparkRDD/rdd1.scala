package SparkRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/11.
  */
object rdd1 {
  /**
    * rdd1的DAG图路径：Resources/image/rdd1_dag.png
    * @param args
    */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD_Basic").setMaster("local")
    val sc = new SparkContext(conf)

    val n = 2000000
    val composite = sc.parallelize(2 to n, 8)
      .map(x => (x, (2 to(n / x))))
      .flatMap(kv => kv._2.map(_ * kv._1))


    val prime = sc.parallelize(2 to n, 8)
      .subtract(composite)

    //prime.foreach(println)
    prime.foreach(line =>{
      println("测试数据："+line)
    })
  }
}
