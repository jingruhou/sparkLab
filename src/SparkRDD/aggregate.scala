package SparkRDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 16-11-28.
  */
object aggregate {
  def main(args: Array[String]): Unit = {
    /**
      * 0:创建环境变量/设置Master地址（本地化处理）/设置应用名称
      */
    val conf = new SparkConf().setAppName("AggregateMethodTest").setMaster("local[*]")
    /**
      * 1:创建SparkContext-sc环境变量实例
      */
    val sc = new SparkContext(conf)
    /**
      * 2:输入数组数据集
      */
    val arr = sc.parallelize(Array(1,2,3,4,5,6))
    /**
      * 3:使用aggregate方法
      */
    val result = arr.aggregate(0)(math.max(_, _),_ + _)
    /**
      * 4:打印结果
      */
    println(result)
  }
}
