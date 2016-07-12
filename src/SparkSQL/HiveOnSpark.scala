package SparkSQL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/6/21.
  */
object HiveOnSpark {
  case class Record(key: Int, value: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HiveOnSpark").setMaster("local")
    val sc = new SparkContext(sparkConf)

    /*val hiveContext = new HiveContext(sc)
    import hiveContext._

    sql("use hive")
    sql("select c.theyear,count(distinct a.ordernumber),sum(b.amount) from tbStock a join tbStockDetail b on a.ordernumber=b.ordernumber join tbDate  c on a.dateid=c.dateid group by c.theyear order by c.theyear")
      .collect().foreach(println)*/

    sc.stop()
  }
}
