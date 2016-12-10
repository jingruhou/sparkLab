package SparkMLLib

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/7/11.
  */
object MLLibBasic {
  def main(args: Array[String]) {
    /**
      * 相关环境配置（appname/master/jars...）
      */
    val conf = new SparkConf().setAppName("SparkMLLib_Basic").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * 0:
      * 创建 稠密/密集向量 <1.0,2.0,3.0>
      * Vectors.dense接收一串值或一个数组
      *
      */
    val denseVec1 = Vectors.dense(1.0,2.0,3.0)
    val denseVec2 = Vectors.dense(Array(1.0,2.0,3.0))

    println("由接收的一串值创建的密集向量: "+denseVec1)
    println("由数组创建的密集向量: "+denseVec2)

    val labelPointVector = LabeledPoint(3,denseVec1)
    println("labelPointVector.features(标记点内容数据): "+labelPointVector.features)
    println("labelPointVector.label(既定标记): "+labelPointVector.label)
    /**
      * 1:
      * 创建 稀疏向量 <1.0,0.0,2.0,0.0>
      * 向量的维度（4） 以及非零位的位置和对应的值
      */
    val sparseVec1 = Vectors.sparse(4,Array(0,2),Array(1.0,2.0))
    println("稀疏向量： "+sparseVec1)

    /**
      * 2:
      * 创建 向量标签
      */
    //创建密集向量
    val vd : Vector = Vectors.dense(2,0,6)

    //对密集向量建立标记点
    val pos = LabeledPoint(1,vd)
    //打印标记点内容数据
    println("pos.features: "+pos.features)
    //打印既定标记
    println("pos.label: "+pos.label)


    //建立稀疏向量
    val vs : Vector = Vectors.sparse(4, Array(0,1,2,3), Array(9,5,2,7))

    //对稀疏向量建立标记点
    val neg = LabeledPoint(2,vs)
    //打印标记点内容数据
    println("neg.features: "+neg.features)
    //打印既定标记
    println("neg.label: "+neg.label)

    /**
      * 总结一下：
      *
      * 以上两种创建向量的方式
      * val denseVec1 = Vectors.dense(1.0,2.0,3.0) 和
      * val vd : Vector = Vectors.dense(2,0,6)）区别在于：
      *
      * 显式指定变量类型（scala语法默认使用的是：类型推断，所以可以不用显式指定变量类型的）
      *
      * 注意：只有标签向量有features和label方法
      *
      *       标签向量是由向量（密集或者稀疏）通过建立标记点构成的
      *
      */

    /**
      * ****************************************************************************************************************
      * ****************************************************************************************************************
      * ****************************************************************************************************************
      */

    /**
      * 2016-12-10 易者悠也
      *
      * 学习 ： LabeledPoint 对象
      *
      *
      *
      * (1)自定义LabeledPoint类：case class LabeledPoint
      * (2)自定义LabeledPoint对象：object LabeledPoint
      * (3)学习调用
      * (4)总结
      *
      */





  }
}
