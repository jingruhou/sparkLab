package SparkRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/12.
  */
object Sample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Sample").setMaster("local")
    val sc = new SparkContext(conf)

    val d = sc.parallelize(1 to 100, 10)

    /**
      * Return a sampled subset of this RDD.
      *
      * Sample：返回RDD的样本子集
      */
    /**
      * Return a sampled subset of this RDD.
      *
      * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
      *
      * @param fraction expected size of the sample as a fraction of this RDD's size
      *
      *  without replacement: probability that each element is chosen; fraction must be [0, 1]
      *
      *  with replacement: expected number of times each element is chosen; fraction must be >= 0
      *
      * @param seed seed for the random number generator
      */
    val result1 = d.sample(false, 0.1, 0)
    val result2 = d.sample(true, 0.1, 0)

    println(result1.toDebugString)

    println("result 1: ")
    result1.collect.foreach(x => print(x + ","))
    println("\nresult 2:")
    result2.collect.foreach(x => print(x + ","))


    //将抽样结果转换为字符串，然后打印出来
    println("result1 : "+result1.collect().mkString(","))
    println("result2 : "+result2.collect().mkString(","))


    /**
      * 2016-12-12 hjr
      *
      * sample(withReplacement,fraction,seed)方法使用实例：
      */
    val data = sc.parallelize(List(1, 2, 3, 3))
    val sampleData = data.sample(false, 0.5)

    sampleData.foreach(println)
    println("采样数据： "+sampleData.collect().mkString(","))


    /**
      * 总结一下
      *
      * 以上两种类型的打印输出集合类型数据方式区别：
      *
      * （1）result1.collect.foreach(x => print(x + ","))
      *
      * （2）println("result1 : "+result1.collect().mkString(","))
      *
      * 方式（1）：将result1这个RDD中的所有元素放进collect集合，然后使用foreach迭代器这个Action操作，
      *
      *            迭代打印每一个元素，并在每一个元素后面添加逗号分割
      *
      * 方式（2）:将result1这个RDD中的所有元素放进collect集合，然后将这个collect数组（包含result1这个RDD中的所有元素）
      *
      *           转化（应该叫做构造字符串：将一个数组类型的数据构造为一个字符串类新的数据,并且可以指定分隔符）为字符串,
      *
      *           并打印输出这个字符串
      *
      *
      *  在大数据背景下，第一种方式明显会慢于第二种方式，因为多增加了一个Action操作，
      *
      *  而方式2仅仅只是一个构造字符串的过程
      *
      */
  }
}
