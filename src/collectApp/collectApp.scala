package collectApp

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/7.
  */
/**
  * 需求说明：
  *
  * 统计日志文件中本周末网购停留总时间超过 2 个小时的女性网民信息
  *
  * 数据说明：
  *
  * LiuYang,female,20
  * YuanJing,male,10
  * GuoYijun,male,5
  * CaiXuyu,female,50
  * Liyuan,male,20
  *
  * 周末两天的日志文件第一列为姓名，第二列为性别，
  * 第三列为本次停留时间，单位为分钟，分隔符为“ ,”
  *
  */
object collectApp {
  def main(args: Array[String]) {
    // 0 初始化环境
    val conf = new SparkConf().setAppName("CollectApplication").setMaster("local")
    val sc = new SparkContext(conf)
    // 1 加载数据
    val data = sc.textFile("Resources/data/collectApp/*")
    // 2 先拿到全部女性数据
    val femaleData = data.filter(_.contains("female"))
    // 3 对同一个女性，对停留时间进行相加
    val reduceTime = femaleData.map(line =>{
      val lineCollect = line.split(",")
      (lineCollect(0),lineCollect(2).toInt)//构造键值对RDD
    }).reduceByKey(_ + _)
    // 4 筛选出网购停留时间大于120分钟的女性,并打印出结果
    val result = reduceTime.filter(line =>{line._2 > 120}).foreach(println)
    /**
      * 精简代码---将以上业务逻辑代码转化为一行代码实现
      */
    val result0 = data.filter(_.contains("female")).map(line =>{(line.split(",")(0),line.split(",")(2).toInt)}).reduceByKey(_ + _).filter(line =>{line._2 > 120}).foreach(println)
    // 5 停止SparkContext
    sc.stop()
  }
}
