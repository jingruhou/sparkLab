package SparkSQL


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Administrator on 2016/6/17.
  */
object Reflection {

  /**
    * 注意：定义的case class在Reflection对象里面，在主函数main的外面
    *
    * 使用一个case class来定义一个schema
    * 包含两个属性：name,age,类型分别是 String,Int
    *
    * @param name
    * @param age
    */
  case class Person(name:String, age:Int)

  def main(args:Array[String]): Unit ={

    // 0 初始化配置环境
    val conf = new SparkConf().setMaster("local").setAppName("Inferring the Schema Using Reflection")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // 1 导入 将一个RDD强制转化为一个DataFrame 的包
    import sqlContext.implicits._

    // 2 创建一个Person对象的RDD
    /**
      * 原始数据：
      * Michael, 29
      * Andy, 30
      * Justin, 19
      */
    val people = sc.textFile("Resources/data/people.txt")
      .map(_.split(","))
      .map(p => Person(p(0),(p(1).trim).toInt))
      .toDF()

    // 3 将该Person对象RDD注册为一张表（“people”）
    people.registerTempTable("people")
    // 显示people表中的数据
    people.show()
   /* +-------+---+
      |   name|age|
      +-------+---+
      |Michael| 29|
      |   Andy| 30|
      | Justin| 19|
      +-------+---+*/

    /**
      * 操作 DataFrame
      *
      * 易者悠也
      */
    // 1 使用sqlContext提供的 sql方法 运行 sql查询语句
    val teenagers = sqlContext.sql("select name,age from people where age >=13 and age <=19")
    // 2 上面sql查询的结果返回的是一个DataFrame
    // 2 这个DataFrame支持所有的普通RDD操作
    // 2 例： 对这个DataFrame使用map操作，拿出第一列数据（Name），循环遍历打印
    teenagers.map(t => "Name: "+t(0)).collect().foreach(println)
    /* Name: Justin */
    // 2 例： 或者使用字段名（“Name”）
    teenagers.map(t => "Name: "+t.getAs[String]("name")).collect().foreach(println)
    /* Name: Justin */

    teenagers.map(_.getValuesMap[Any](List("name","age"))).collect().foreach(println)
    /* Map(name -> Justin, age -> 19) */

  }
}
