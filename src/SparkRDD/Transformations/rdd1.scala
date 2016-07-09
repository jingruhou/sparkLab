package SparkRDD.Transformations

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/8.
  */
object rdd1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("hjr").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      *并行集合操作实例
      *
      *
      * 并行集合（Parallelized collections）的创建是通过在一个已有的集合（Scala Seq）上调用SparkContext的parallelize方法实现的
      *
      * 集合中的元素被复制到一个可并行操作的分布式数据集中
      *
      * 一旦创建完成，这个分布式数据集（distData）就可以被并行操作
      *
      * 我们可以调用distData.reduce((a, b) => a + b),将数组中的元素相加
      * 或者这种形式也行：distData.reduce((_ + _))
      *
      * 并行集合一个很重要的的参数是切片数（slices），表示一个数据集切分的份数
      * Spark会在集群上为每一个切片运行一个任务，我们可以在集群上为每个CPU设置2-4个切片（slices）
      * 正常情况下，Spark会试着基于你的集群状况自动的设置切片的数目，
      * 然而，我们也可以通过parallelize的第二个参数手动设置（eg:sc.parallelize(data, 10)）
      */
    val data1 = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data1)
    println(distData.reduce((a, b)=> a + b))
    println(distData.reduce((_+_)))

    /**
      * flatmap算子操作实例
      */
    val data = sc.textFile("Resources/data/1.txt")
    data.foreach(println)

    data.flatMap(line =>{
      line.split("\t")(2).split(",").map(codeline =>{line.split("\t")(0)+"\t"+line.split("\t")(1)+"\t"+codeline})
    }).foreach(println)

    data.flatMap(line =>(line.split("\t"))).map(word =>(word, 1)).reduceByKey(_ + _).foreach(println)

    data.flatMap(line =>(line.split("\t"))).map(word =>(word,1)).reduceByKey((a, b) => a + b).foreach(println)



  }
}
