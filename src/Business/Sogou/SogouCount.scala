package Business.Sogou

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/7/13.
  */
object SogouCount {
  def main(args: Array[String]) {
    /**
      *  0 初始化环境配置（本地模式运行）
      */
    val conf = new SparkConf().setAppName("SogouCount").setMaster("local")
    val sc = new SparkContext(conf)
    /**
      * 1 加载数据---原始数据格式如下(5000条数据)
      *
      * 数据格式为：
      * 访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
      *
      * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，
      * 即同一次使用浏览器输入的不同查询对应同一个用户ID。
      *
      * 00:04:13	7543715252866381	[qq华夏外挂]	2 8	www.ipk.cn/sort/111_1.htm
      * 00:04:13	2342593187087721	[51sese]	3 2	sese.dyxs51.com/
      * 00:04:13	5114779875937814	[Dmplay弹广告软件]	3 3	zhidao.baidu.com/question/23240347
      * 00:04:13	30514361700243453	[企业固定资产明细表]	4 15	www.cg163.net/sorthtm/d.aspid=28208.htm
      * 00:04:13	14130930299333993	[火车时刻表]	1 1	qq.ip138.com/train/
      * 00:04:13	2537807345564703	[湖北宜昌黑恶势力]	7 4	past.people.com.cn/GB/other4583/4601/4641/20010605/482535.html
      * 00:04:13	7707338017752476	[四川电视台主持人宁远]	7 18	www.zs.cdut.edu.cn/bencandy.php?fid=27&aid=635
      * 00:04:13	05818080269683851	[怎样容易生男孩]	1 3	zhidao.baidu.com/question/4405948
      * 00:04:13	7166906396513426	[11sss.com]	1 8	11sss.com/
      * 00:04:13	4402345957890734	[贵州水灾]	2 3	www.chinamil.com.cn/site1/xwpdxw/2008-05/29/content_1288754.htm
      * 00:04:13	8613750710704402	[十字绣]	9 7	www.2005-01-01.com/
      * 00:04:14	1922848153437114	[大学习大讨论]	2 1	news.xinhuanet.com/legal/2008-03/19/content_7821431.htm
      * 00:04:14	2073404324811946	[蒙古国总统是谁]	1 3	ks.cn.yahoo.com/question/1406090709262.html
      */
    val data = sc.textFile("Resources/data/Sogou/SogouQ1.txt")
    println("原始数据的条数："+data.count())
    /**
      * 1 - 1 输出原始数据(只拿10条数据)
      *
      * 这种方式是将所有数据放到collect里面去的，然后再循环遍历打印出来
      * 数据量不大的话（几十万条），可以做此选择；
      * 如果有千万条数据的话，不建议使用
      * 超过了java/scala中Array的最大长度限制
      */
    data.collect().take(10).foreach(println)
    /**
      * collect方法定义如下：
      *
      * Return an array that contains all of the elements in this RDD.
      *
      * def collect(): Array[T] = withScope {
      * val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
      * Array.concat(results: _*)
    }**/

    /**
      * 1 - 2 输出原始数据（只拿10条数据）
      *
      * 这种方式直接打印RDD里面的所有数据，不用存储到数组里面去
      */
    data.take(10).foreach(println)
    /**
      * foreach方法定义如下：
      *
      * Applies a function f to all elements of this RDD.
      *
      *  def foreach(f: T => Unit): Unit = withScope {
      *  val cleanF = sc.clean(f)
      *  sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
      *  }
      */
    /**
      * 2 - 1 输出字段个数等于6的条数
      *
      * Scala闭包的方式实现
      */
    val data1 = data.map(_.split("\t")).filter(_.length == 6)
    /**
      * 2 - 2 输出字段个数等于6的条数
      *
      * 正常实现
      */
    val data1_1 = data.map(line =>{
      line.split("\t")
    }).filter(cols => cols.length == 6)
    //打印字段个数等于6的条数
    println("（闭包方式：）打印字段个数等于6的条数: "+data1.count())
    println("（正常方式：）打印字段个数等于6的条数: "+data1_1.count())

    /**
      * 3 输出搜索结果排名第二的条数
      */
    val rdd1 = data.map(_.split("\t")).filter(_.length==6)
    println("字段个数等于6的条数为："+rdd1.count())
    println("-------------------输出搜索结果排名第2的条数-------------------")
    val rdd2 = rdd1.filter(_(3).toInt == 2)

    /**
      * 关闭SparkContext
      */
    sc.stop
  }
}
