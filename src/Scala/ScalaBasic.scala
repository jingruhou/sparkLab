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

    /**
      * 8 类
      *
      * 下面的例子展示了如何在类中用def定义方法 和 使用val定义字段值
      *
      * 方法就是可以访问类的状态的函数
      */
    class Calculator{
      val brand: String = "houjingru_www.xunfang.com"
      def add(m : Int,n : Int) : Int = m + n
    }
    val calc = new Calculator
    println("类的使用1： "+calc.add(1, 2))
    println("类的使用2： "+calc.brand)


    /**
      * 9 构造函数
      *
      * 构造函数不是特殊的方法，他们是除了类的方法定义之外的代码。
      *
      * 让我们扩展计算器的例子，增加一个构造函数参数，
      * 并且用它来初始化内部状态
      */
    class Calculator_(brand:String){
      // A constructor.
      val color:String = if (brand == "IT"){
        "blue"
      }else if (brand == "HP"){
        "black"
      }else{
        "white"
      }

      // An instance method
      def add(m:Int, n:Int):Int = m + n
    }

    //使用构造函数创造一个实例
    val calc_ = new Calculator_("HP")
    println("构造函数实例： "+calc_)
    println("构造函数应用： "+calc_.color)

    /**
      * 10 表达式
      *
      * 上面的Calculator例子说明了Scala是如何面向表达式的。
      * 颜色的值就是绑定在一个if/else表达式上的。
      *
      * Scala是高度面向表达式的，
      * 大多数东西都是表达式而非指令。
      */

    /**
      * 11 旁白：函数 vs 方法
      *
      * 函数和方法在很大程度上是可以互换的。
      * 由于函数和方法是如此的相似，
      * 你可能都不知道你调用的东西是一个函数还是一个方法。
      * 而当真正碰到的方法和函数之间的差异的时候，你可能会感到困惑。
      */
    class C {
      var acc = 0
      def minc = { acc += 1}
      val finc = {() => acc +=1}
    }
    /**
      * 当你可以调用一个不带括号的“函数”，但是对另一个却必须加上括号的时候，
      * 你可能会想哎呀，我还以为自己知道Scala是怎么工作的呢。
      * 也许他们有时需要括号？你可能以为自己用的是函数，但实际使用的是方法。
      *
      * 在实践中，即使不理解方法和函数上的区别，你也可以用Scala做伟大的事情。
      * 如果你是Scala新手，而且在读两者的差异解释，你可能会跟不上。
      * 不过，这并不意味着你在使用Scala上有麻烦。
      * 它只是意味着函数和方法之间的差异是很微妙的，只有深入语言内部才能清楚理解它。
      *
      */
    val c = new C
    println("函数： "+c.minc)
    println("方法： "+c.finc)

    /**
      * 12 继承
      *
      * 参考Effective Scala指出如果子类与父类实际上没有区别，
      * 类型别名是优于继承的。
      * A Tour of Scala 详细介绍了子类化
      */
    class ScientificCalculator(brand:String) extends Calculator_(brand){
      def log(m:Double, base:Double) = math.log(m)/math.log(base)
    }

    /**
      * 13 重载方法
      *
      *
      */
    class EvenMoreScientifiCalculator(brand:String) extends ScientificCalculator(brand){
      def log(m: Int):Double = log(m, math.exp(1))
    }

    /**
      * 14 抽象类
      *
      * 你可以定义一个抽象类，它定义了一些方法但没有实现他们。
      * 取而代之是由扩展抽象的子类定义这些方法，
      * 你不能创建抽象类的实例。
      */
    abstract class Shape{
      def getArea():Int  //subclass should define this
    }
    class Cricle(r: Int) extends Shape{
      def getArea():Int={r * r * 3}
    }
    val cricle = new Cricle(2)
    println("抽象类的使用： "+cricle)

    /**
      * 15 特质 （Traits）
      *
      * 特质是一些字段和行为的集合，它可以扩展或混入（mixin）你的类中
      */
    trait Car{
      val brand: String
    }

    trait Shiny{
      val shineRefraction:Int
    }
    /**
      * class BMW extends Car{
      *
      * val brand = "BMW"
      * }
      *
      */
    //通过with关键字，一个类可以扩展多个特质：
    class BMW extends Car with Shiny{
      val brand = "BMW"
      val shineRefraction = 12
    }

    /**
      * 经验小结：
      *
      * 什么时候应该使用特质而不是抽象类？如果你想定义一个类似接口的类型，
      * 你可能会在特质和抽象类之间难以取舍。
      * 这两种形式都可以让你定义一个类型的一些定义行为，
      * 并要求继承者定义一些其他行为。一些经验法则：
      *
      * （1）优先使用特质：一个类扩展多个特质是很方便的，但却只能扩展一个抽象类。
      * （2）如果你需要构造函数参数，使用抽象类。因为抽象类可以定义带参数的构造函数，而特质不行。
      * 例如，你不能说trait t(i: Int){},参数i是非法的。
      */


    /**
      * 16 类型
      *
      * 此前，我们定义了一个函数的参数为Int，表示输入是一个数字类型。
      * 其实函数也可以是范型的，来适用于所有类型。
      * 当这种情况发生时，你会看到用方括号语法引入的类型参数。
      *
      * 下面的例子展示了一个使用范型键和值的缓存。
      */
    trait Cache[K, V]{
      def get(key: K):V
      def put(key: K, value: V)
      def delete(key: K)
    }
    //方法也可以引入类型参数
    def remove[K](key: K){}

  }
}
