package DTSpark.Scala.Basics

/**
  * Created by hjr on 16-12-28.
  */

trait Logger{
  def log(message:String){
    println("Logger: "+message)
  }
}
trait RichLogger extends Logger{
  override def log(message:String){
    println("RichLogger: "+message)
  }
}
class  Loggin(val name:String) extends Logger{
  def loggin {
    println("Hi, welcome ! " + name)
  log(name)
  }
}
object HelloTrait {
  def main(args: Array[String]): Unit = {
    val personLoggin = new Loggin("DTSpark") with RichLogger
    personLoggin.loggin
  }
}
