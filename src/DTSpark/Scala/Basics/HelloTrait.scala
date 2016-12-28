package DTSpark.Scala.Basics

/**
  * Created by hjr on 16-12-28.
  */

/**
  * trait接口：Logger，里面没有抽象的方法，而是具体的方法实现
  *
  * 此处，Logger接口里面包含有一个log()方法，该方法的具体行为：打印输入的数据（Logger+“输入数据”）
  *
  * def log(message:String){***}该方法输入参数为一个String类型的message
  */
trait Logger{
  def log(message:String){
    println("Logger: "+message)
  }
}

/**
  * trait接口：RichLogger
  *
  * RichLogger接口继承Logger接口，
  *
  * 然后RichLogger接口的具体实现方法为：覆写Logger接口的log方法，关键字override标记
  *
  * 覆写的log(message：String){***}方法的具体行为：打印一行数据（该行为可以和Logger接口的log方法的行为不一样）
  *
  * 打印的数据为：RichLogger+“输入数据”
  */
trait RichLogger extends Logger{
  override def log(message:String){
    println("RichLogger: "+message)
  }
}

/**
  * 自定义Loggin登录类，该类继承Logger接口，输入参数为name：String
  *
  * Loggin类里面包含一个loggin方法，该方法没有输入参数，只有具体行为
  *
  * 行为包括：首先打印（“Hi, welcome ! ”+ “类的输入参数《可以理解为类的构造函数》”）
  *
  *           然后调用log(name)方法，该方法是继承的Logger接口里面的具体方法
  *
  */
class  Loggin(val name:String) extends Logger{
  def loggin {
    println("Hi, welcome ! " + name)
    log(name)
  }
}
trait Information{
  def getInformation : String
  def checkIn : Boolean = {
    getInformation.equals("Spark")
  }
}
class Passenger(val name : String) extends Information{
  def getInformation = name
}
/**
  * 伴生对象相关
  */
object HelloTrait {
  //主函数定义
  def main(args: Array[String]): Unit = {
    /**
      *实例化一个Loggin登录类的对象，该对象with“混入”RichLogger接口
      *
      * 所以，该对象就会拥有RichLogger接口的方法，而不再拥有Logger接口的方法了
      */
    val personLoggin = new Loggin("DTSpark") with RichLogger

    /**
      * 调用 实例化的Loggin登录类的对象的 loggin方法
      *
      * Loggin登录类相当于继承了RichLogger类，而不再是定义的Logger类了
      *
      * 所以，打印的结果就是："RichLogger: "+message
      */
    personLoggin.loggin

    /**
      *     结果如下：
      *
      *     Hi, welcome ! DTSpark
      *     RichLogger: DTSpark
      *
      */
  }
}
