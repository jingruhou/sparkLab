package StreamingMLLib.KMeans

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

/**
  * Created by hjr on 17-1-27.
  */
object TestKmeansDataGenerator {
  def index(length:Int)={
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    /**
      * 1 调用该模拟器需要的三个参数，分别为文件路径、端口号、间隔时间（单位：毫秒）
      *
      */
    if(args.length!=3){
      System.err.println("Usage:<filename><port><milliseconds>")
      System.exit(1)
    }

    /**
      * 2 获取指定文件总的行数
      */
    val filename = args(0)
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    /**
      * 3 指定监听某端口，当外部程序请求时建立链接
      */
    val listener = new ServerSocket(args(1).toInt)
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run = {
          println("Got client connected from: "+socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(),true)
          while(true){
            Thread.sleep(args(2).toLong)
            //当该端口接受请求时，随机获取某行数据发送给对方
            val content = lines(index(filerow))
            println("时间："+System.currentTimeMillis()+" 发送模型测试数据："+content)
            out.write(content+"\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
