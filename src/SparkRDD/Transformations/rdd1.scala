package SparkRDD.Transformations

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/8.
  */
object rdd1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("hjr").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:/sparkLab/Resources/data/1.txt")
    data.foreach(println)

    data.flatMap(line =>{
      line.split("\t")(2).split(",").map(codeline =>{line.split("\t")(0)+"\t"+line.split("\t")(1)+"\t"+codeline})
    }).foreach(println)

    data.flatMap(line =>(line.split("\t"))).map(word =>(word, 1)).reduceByKey(_ + _).foreach(println)

    data.flatMap(line =>(line.split("\t"))).map(word =>(word,1)).reduceByKey((a, b) => a + b).foreach(println)
  }
}
