package SparkRDD

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/8.
  */
object rdd0 {
  def main(args:Array[String]): Unit ={
    /**
      * Create a SparkConf that loads defaults from system properties and the classpath
      * 通过SparkConf()来初始化配置环境（比如appname、运行的master、jars等）
      */
    val conf = new SparkConf().setAppName("rdd_Parallelize").setMaster("local[2]")
    /**
      * 初始化Spark最重要的SparkContext对象，参数为SparkConf对象
      * SparkContext是Spark“引擎”的主要入口
      * 一个SparkContext呈现了一个Spark集群的连接，
      * 并且用它来在一个集群上创建RDD、累加器和广播变量
      *
      * 每一个jvm虚拟机仅仅只能有一个SparkContext对象是活跃的状态
      * 你在创建一个新的SparkContext对象之前，必须使用“stop()”方法来停止这个SparkContext对象的活跃状态
      * 这个限制可能在最终的版本中被移除，详见SPARK-2243
      *
      * 参数SparkConf对象/变量，“配置”了一个Spark配置对象，该对象描述了一个应用程序的配置
      * 在这个配置对象里面的设置将覆盖默认的配置和系统属性
      *
      * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
      * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
      *
      * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
      * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
      *
      * ***param config a Spark Config object describing the application configuration. Any settings in
      *   this config overrides the default configs as well as system properties.
      */
    val sc = new SparkContext(conf)

    //设置打印日志级别
    //Logger.getRootLogger.setLevel(Level.WARN)
    // 2.1节
    // 2.1.1 RDD 创建操作
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val distData = sc.parallelize(data, 3)

    /**
      * 加载数据的几种方式：
      val distFile1 = sc.textFile("data.txt") // 本地当前目录下的文件
      val distFile2 = sc.textFile("hdfs://192.168.1.100:9000/input/data.txt") // HDFS 文件
      val distFile3 = sc.textFile("file:/input/data.txt") // 本地指定目录下的文件
      val distFile4 = sc.textFile("/input/data.txt") // 本地指定目录下的文件
    */

    // 2.1.2 RDD 转换操作
    /**
      * Distribute a local Scala collection to form an RDD
      * SparkContext的parallelize方法会通过本地的一个scala集合构造一个RDD
      * 第一个参数：scala的集合
      * 第二个参数：数据分片的数量
      */
    val rdd1 = sc.parallelize(1 to 9, 3)
    /**
      * rdd的map算子操作
      * Return a new RDD by applying a function to all elements of this RDD
      * 应用一个函数给这个RDD的所有元素/每一个元素，并返回一个新的RDD
      * 其中：x => x * 2 为一个函数，具体了解scala语法
      */
    val rdd2 = rdd1.map(x => x * 2)
    /**
      * Return an array that contains all of the elements in this RDD
      * 返回一个包含了这个rdd的所有元素的数组
      *
      * 注意：这个操作就是将所有worker上的所有数据加载到master上面来“计算”
      * 大数据量非常不建议这么做，小数据量可以测试使用
      */
    rdd2.collect

    /**
      * rdd的filter算子操作
      * Return a new RDD containing only the elements that satisfy a predicate.
      * 返回 包含这个RDD里面 仅仅满足条件的元素 所构建的新的RDD
      * 其中：x => x > 10为一个函数，具体详见scala语法
      */
    val rdd3 = rdd2.filter(x => x > 10)
    rdd3.collect.foreach(println)

    /**
      * rdd的flatMap算子操作
      * Return a new RDD by first applying a function to all elements of this
      * RDD, and then flattening the results.
      * 返回一个新得RDD：（这个RDD来自于）首先将一个函数应用于这个RDD的所有元素/每一个元素，
      * 然后在“扁平化”这个结果results
      *
      * 详查：flat扁平化操作
      */
    val rdd4 = rdd3.flatMap(x => x to 20)
    rdd4.foreach(println)

    /**
      * rdd的mapPartitions算子操作
      * Return a new RDD by applying a function to each partition of this RDD
      * 返回一个新的RDD：（这个RDD来自于）将一个函数应用于这个RDD的每一个分区(partition)
      * 其中：myfunc为自定义函数，myfunc的参数为Iterator迭代器，详见后面
      *
      * 注：日后继续详细学习mapPartitions算子
      */
    val rdd5 = rdd1.mapPartitions(myfunc)
    rdd5.collect

    val a = sc.parallelize(1 to 10000, 3)
    /**
      * rdd的sample算子操作
      * Return a sampled subset of this RDD.
      * 返回这个rdd的抽样的样本子集
      * 参数一：是否有放回
      * 参数二：待查...
      * 参数三：随机数生成器的seed
      * **param withReplacement can elements be sampled multiple times (replaced when sampled out)
      * **param fraction expected size of the sample as a fraction of this RDD's size
      *  without replacement: probability that each element is chosen; fraction must be [0, 1]
      *  with replacement: expected number of times each element is chosen; fraction must be >= 0
      * **param seed seed for the random number generator
      */
    a.sample(false, 0.1, 0).count

    /**
      * rdd的union算子操作
      * Return the union of this RDD and another one. Any identical elements will appear multiple
      * times (use `.distinct()` to eliminate them).
      * 返回这个rdd和另外一个rdd的union结果（并集）构成的新的rdd
      * 该union算子返回结果里面 同一个元素将会出现多次
      * 可以使用distinct()方法来消除（重复的元素）
      */
    val rdd8 = rdd1.union(rdd3)
    rdd8.collect
    rdd8.foreach(println)

    /**
      * rdd的intersection算子操作
      * * Return the intersection of this RDD and another one. The output will not contain any duplicate
      * elements, even if the input RDDs did.
      *
      * Note that this method performs a shuffle internally.
      * 返回这个rdd和另一个rdd的intersection结果（交集）构成的新的rdd
      * 该intersection算子返回结果里面  将不包含任何重复的元素，
      * 即使是输入的rdd里面有重复的元素，返回的结果里面也没有
      */
    val rdd9 = rdd8.intersection(rdd1)
    rdd9.collect
    rdd9.foreach(println)

    /**
      * rdd的distinct算子操作
      * Return a new RDD containing the distinct elements in this RDD.
      * 返回一个新的rdd：包含了原来rdd里面的所有独特的元素（没有重复值）
      */
    val rdd10 = rdd8.union(rdd9).distinct
    rdd10.collect
    rdd10.foreach(println)

    val rdd0 = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)), 3)
    /**
      * rdd的groupByKey()算子操作
      * 将一个rdd按照key进行分组：将所有key相同的值放进一个单独的序列
      * 将采用Hash分区来代替已经存在（原来的）的分区级别
      * 每一个分组里面的元素的排序是不被保证的(没有排序)
      * 可能每次执行的结果RDD是不能用来评估的
      *
      * 注意：groupByKey算子操作的前提是键值对类型的RDD
      *
      * Group the values for each key in the RDD into a single sequence. Hash-partitions the
      * resulting RDD with the existing partitioner/parallelism level. The ordering of elements
      * within each group is not guaranteed, and may even differ each time the resulting RDD is
      * evaluated.
      *
      * Note: This operation may be very expensive. If you are grouping in order to perform an
      * aggregation (such as a sum or average) over each key, using [[org.apache.spark.rdd.PairRDDFunctions.aggregateByKey]]
      * or [[org.apache.spark.rdd.PairRDDFunctions.reduceByKey]] will provide much better performance.
      */
    val rdd11 = rdd0.groupByKey()
    /**
      * 0 循环遍历打印这个rdd
      * result的形式为：(key,valuesSequence)
      */
    rdd11.foreach(println)
    //(1,CompactBuffer(1, 2, 3))
    //(2,CompactBuffer(1, 2, 3))
    /**
      * 1 循环遍历打印这个rdd
      * result为键值对形式的rdd（scala -- Tuple）
      * 可以根据._1(._2)随意取key值或者valuesSequence值
      */
    rdd11.foreach(x => println("groupByKey:"+x._1+"--"+x._2))
    //groupByKey:1--CompactBuffer(1, 2, 3)
    //groupByKey:2--CompactBuffer(1, 2, 3)

    /**
      * rdd的reduceByKey算子操作
      * 用一个可关联的和可替换的聚合函数 来 合并每一个key对应的values所有值
      * “在发送结果到聚合函数之前，先将在本地执行这个（两两）合并操作，”---（待修改）
      * 类似于MR里面的“combiner/组合器”
      *
      * Merge the values for each key using an associative and commutative reduce function. This will
      * also perform the merging locally on each mapper before sending results to a reducer, similarly
      * to a "combiner" in MapReduce.
      */
    val rdd12 = rdd0.reduceByKey((x, y) => x + y)
    rdd12.foreach(x => println("reduceByKey:"+x))
    //reduceByKey:(1,6)
    //reduceByKey:(2,6)
    rdd12.foreach(x => println("reduceByKey"+x._1+":"+x._2))
    //reduceByKey1:6
    //reduceByKey2:6

    // val z = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
    //z.aggregate(0)(math.max(_, _),  _ + _)
    val z = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3)))
    /**
      * rdd的aggregateByKey算子操作
      * 用给定的联合函数和一个中间值（0值） 来 聚合每一个key的值
      * 这个给定的联合函数能够返回一个不同的类型的result
      * Aggregate the values of each key, using given combine functions and a neutral "zero value".
      * This function can return a different result type, U, than the type of the values in this RDD,
      * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
      * as in scala.TraversableOnce. The former operation is used for merging values within a
      * partition, and the latter is used for merging values between partitions. To avoid memory
      * allocation, both of these functions are allowed to modify and return their first argument
      * instead of creating a new U.
      */
    z.aggregateByKey(0)(math.max(_, _), _ + _).foreach(x => println("aggregateByKey:"+x))
    //aggregateByKey:(2,3)
    //aggregateByKey:(1,7)

    val data2 = Array((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0))
    val rdd = sc.parallelize(data2, 2)
    /**
      * rdd的combineByKey算子操作（有待继续研究）
      * 用一个自定义的聚合函数 根据每一个key来联合每一个元素
      *
      * 此处的这个方法是向后兼容的，对于shuffle（洗牌）操作不提供联合类标签信息
      *
      * Generic function to combine the elements for each key using a custom set of aggregation
      * functions. This method is here for backward compatibility. It does not provide combiner
      * classtag information to the shuffle.
      */
    val combine1 = rdd.combineByKey(createCombiner = (v: Double) => (v: Double, 1),
      mergeValue = (c: (Double, Int), v: Double) => (c._1 + v, c._2 + 1),
      mergeCombiners = (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
      numPartitions = 2)
      combine1.collect

    /**
      * rdd的sortByKey算子操作
      * 按照key对这个rdd进行排序，每一个分区包含了这个分区范围的排序数据
      * 在这个resulting RDD上 调用collect或者save方法 来 返回或者输出一个已经排好序的记录列表
      * (如果调用save方法：它们（resulting RDD）将被写入到文件系统里的多个“分片”文件,用keys来排序)
      *
      * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
      * `collect` or `save` on the resulting RDD will return or output an ordered list of records
      * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
      * order of the keys).
      */
    val rdd14 = rdd0.sortByKey()
    rdd14.foreach(println)
    /*
    (1,1)
    (2,1)
    (1,2)
    (2,2)
    (1,3)
    (2,3)
    */
    /**
      * 思考：上面的打印结果的顺序和下面打印结果的顺序不同的原因
      */
    rdd14.foreach(x => println("sortByKey:"+x))
    /*
    sortByKey:(1,1)
    sortByKey:(1,2)
    sortByKey:(1,3)
    sortByKey:(2,1)
    sortByKey:(2,2)
    sortByKey:(2,3)
    */
    rdd14.foreach(x => println(x))
    /*
    (2,1)
    (1,1)
    (2,2)
    (1,2)
    (1,3)
    (2,3)
    */

    /**
      * rdd的join算子操作
      *
      *
      * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
      * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
      * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
      */
    //val rdd0 = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)), 3)
    val rdd15 = rdd0.join(rdd0)
    rdd15.foreach(x =>println("join:"+x))
    /*
    join:(1,(1,1))
    join:(1,(1,2))
    join:(1,(1,3))
    join:(1,(2,1))
    join:(1,(2,2))
    join:(1,(2,3))
    join:(1,(3,1))
    join:(1,(3,2))
    join:(1,(3,3))
    */

    val rdd16 = rdd0.cogroup(rdd0)
    rdd16.collect

    val rdd17 = rdd1.cartesian(rdd3)
    rdd17.collect

    //val rdd18 = sc.parallelize(1 to 9, 3)
    //rdd18.pipe("head -n 1").collect

    val rdd19 = rdd1.randomSplit(Array(0.3, 0.7), 1)
    rdd19(0).collect
    rdd19(1).collect

    val rdd20_1 = sc.parallelize(1 to 9, 3)
    val rdd20_2 = sc.parallelize(1 to 3, 3)
    val rdd20_3 = rdd20_1.subtract(rdd20_2)

    val rdd21_1 = sc.parallelize(Array(1, 2, 3, 4), 3)
    val rdd21_2 = sc.parallelize(Array("a", "b", "c", "d"), 3)
    val rdd21_3 = rdd21_1.zip(rdd21_2)

    val data3 = Array((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0))
    val rdd24 = sc.parallelize(data3, 2)
    val combine24_1 = rdd24.combineByKey(createCombiner = (v: Double) => (v: Double, 1),
      mergeValue = (c: (Double, Int), v: Double) => (c._1 + v, c._2 + 1),
      mergeCombiners = (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
      numPartitions = 2)
    val treeAggregate24_1 = rdd24.treeAggregate((0, 0.0))(seqOp = ((u, t) => (u._1 + t._1, u._2 + t._2)),
      combOp = (u1, u2) => (u1._1 + u2._1, u1._2 + u2._2),
      depth = 2)

    // 2.1.3 RDD 行动操作
    val rdd3_1 = sc.parallelize(1 to 9, 3)
    val rdd3_2 = rdd3_1.reduce(_ + _)
    rdd3_1.collect()
    rdd3_1.count()
    rdd3_1.first()
    rdd3_1.take(3)
    rdd3_1.takeSample(true, 4)
    rdd3_1.takeOrdered(4)

  }
  def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }
}
