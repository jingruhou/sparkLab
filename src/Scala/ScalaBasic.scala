package Scala

/**
  * Created by hjr on 17-8-12.
  */
object ScalaBasic {
  def main(args: Array[String]): Unit = {
    /**
      * 0 表达式
      *
      * (Scala(几乎)一切都是表达式)
      */
    println(1 + 1)

    /**
      * 1 值
      *
      * 可以给一个表达式的结果起个名字，赋成一个不变量（val）
      *
      * 不能改变这个不变量的值
      */
    val two = 1 + 1
    println(two)

    /**
      * 2 变量
      *
      * 如果需要修改这个名称和结果的绑定，可以选择使用 var
      */
    var name = "steve"
    println(name)

    name = "marius"
    println(name)

    /**
      * 3 函数
      *
      * 使用def定义函数
      */
    def addOne(m: Int):Int = m + 1

    val three = addOne(2)
    println(three)

    //如果函数不带参数，可以不用写括号
    def three_() = 1 + 2
    println("函数不带参数，写括号执行： "+three_())
    println("函数不带参数，不用写括号执行 ： "+three_)

    /**
      * 4 匿名函数
      *
      */

    //这个函数为 名为x的Int变量加1
    val res2 = (x:Int) => x+1
    println("匿名函数使用： "+res2(1))

    //可以传递匿名函数，或者将其保存成不变量
    val addOne_ = (x:Int) => x + 1
    println("匿名函数（将匿名函数保存成val不变量）： "+addOne_(1))

    //如果函数有许多表达式，可以使用{}来格式化代码，使之容易读
    def timesTwo(i: Int):Int = {
      println("hello world")
      i * 2
    }
    // 匿名函数也是一样的(函数有许多表达式，可以使用{}来格式化代码)
    val timesTwo_ = {
      i:Int =>
        println("hello world")
        i * 2
    }

    println("匿名函数（包含多行表达式）： "+timesTwo_(2))

    /**
      * 5 部分/局部应用（Partial application）
      *
      * 可以使用“_”部分应用一个函数，结果将得到另一个函数。
      *
      *  Scala使用下划线表示不同上下文中的不同事物，
      *  可以通常把它看做是一个没有命名的神奇通配符。
      *
      *  在{_ + 2}的上下文中，它代表一个匿名函数
      */
    def adder(m:Int,n:Int) = m + n
    val add2 = adder(2,_:Int)

    println("部分应用（“_”）： "+add2(3))

    /**
      * 6 柯里化函数
      *
      * 有时会有这样的需求：允许别人一会在你的函数上应用一些参数，
      * 然后又应用另外的一些参数。
      *
      * 例如：
      * 一个乘法函数，在一个场景需要选择乘数，
      * 而另一个场景需要选择被乘数
      */
    def multiply(m:Int)(n:Int):Int = m * n
    //可以直接传入两个参数
    println("柯里化函数： "+multiply(2)(3))

    //也可以填上第一个参数并且部分应用第二个参数
    val timesTwo1 = multiply(2) _
    println("柯里化函数应用（部分应用第二个参数） "+timesTwo1(3))

    //可以对任何多参数函数执行柯里化，例如之前的adder函数
    (adder _).curried
    println("对函数执行柯里化： "+(adder _).curried)

    /**
      * 7 可变长度参数
      *
      * 这是一个特殊的语法，可以向方法传入任意多个同类型的参数。
      *
      * 例如，
      * 要在多个字符串上执行String的capitalize函数，可以这样写：
      */
    def capitalizeAll(args:String*) = {
      args.map{
        arg => arg.capitalize //返回此字符串，第一个字符转换为大写
      }
    }
    println("可变长度参数应用(2个参数)： "+capitalizeAll("hello","world"))
    println("可变长度参数应用(3个参数)： "+capitalizeAll("hello","xunfang","houjingru"))

  }
}
