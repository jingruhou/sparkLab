package SparkRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/12.
  */
object Cogroup {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cogroup").setMaster("local")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(List(1,2,1,3),2)
    val b = sc.parallelize(List(1,2,3,4,5,6),3)

    val d = a.map((_,"b"))
    d.foreach(println)//(1,b)(2,b)(1,b)(3,b)

    val e = b.map((_,"c"))
    e.foreach(println)//(1,c)(2,c)(3,c)(4,c)(5,c)(6,c)

    /**
      * Cogroup
      * 将多个RDD中同一个Key对应的Value组合到一起
      */
    val result = d.cogroup(e,4)
    result.foreach(println)
    //(4,(CompactBuffer(),CompactBuffer(c)))
    //(1,(CompactBuffer(b, b),CompactBuffer(c)))
    //(5,(CompactBuffer(),CompactBuffer(c)))
    //(6,(CompactBuffer(),CompactBuffer(c)))
    //(2,(CompactBuffer(b),CompactBuffer(c)))
    //(3,(CompactBuffer(b),CompactBuffer(c)))

    println(result.toDebugString)
    /*
    (4) MapPartitionsRDD[5] at cogroup at Cogroup.scala:22 []
    |  CoGroupedRDD[4] at cogroup at Cogroup.scala:22 []
    +-(2) MapPartitionsRDD[2] at map at Cogroup.scala:16 []
    |  |  ParallelCollectionRDD[0] at parallelize at Cogroup.scala:13 []
    +-(3) MapPartitionsRDD[3] at map at Cogroup.scala:19 []
    |  ParallelCollectionRDD[1] at parallelize at Cogroup.scala:14 []
    */

    val data1 = sc.parallelize(List((1, "zkjz"), (2, "kmust")))
    val data2 = sc.parallelize(List((1, "yiyou"), (2, "yiyou"), (3, "very")))
    val data3 = sc.parallelize(List((1, "houjingruru"), (2, "houjingru"), (3, "good")))
    val result1 = data1.cogroup(data2, data3)
    result1.collect.foreach(println)
    /*
    (1,(CompactBuffer(zkjz),CompactBuffer(yiyou),CompactBuffer(houjingruru)))
    (3,(CompactBuffer(),CompactBuffer(very),CompactBuffer(good)))
    (2,(CompactBuffer(kmust),CompactBuffer(yiyou),CompactBuffer(houjingru)))
    */
    //从上面的结果可以看到，data1中不存在Key为3的元素（自然就不存在Value了），
    // 在组合的过程中将data1对应的位置设置为CompactBuffer()了，而不是去掉了
  }
}
