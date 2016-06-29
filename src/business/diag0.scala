package business

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/29.
  */
object diag0 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("DiagAPP")
    val sc = new SparkContext(conf)

    // 0 加载原始数据
    val outclinical_diag2 = sc.textFile("D:/streamingData/diag/outclinical_diag2.txt")
    val outclinical_words0616 = sc.textFile("D:/streamingData/diag/outclinical_words0616.txt")



    // 1 数据采样
    outclinical_diag2.take(10).foreach(println)
   /* 原始门诊数据格式
   "RID"	"MPI_PERSON_ID"	"VISITOR_DATE"	"DIAG_NAME"
    4339473	"9999804"	"2013-05-04 00:00:00.0"	"湿疹样皮炎"
    1202256	"9999804"	"2015-04-17 00:00:00.0"	"胃溃疡"
    1202257	"9999804"	"2015-04-17 00:00:00.0"	"冠心病"
    6896042	"9999804"	"2015-04-27 00:00:00.0"	"糖尿病"
    1183334	"9999804"	"2015-05-02 00:00:00.0"	"高血压病"
    1183335	"9999804"	"2015-05-02 00:00:00.0"	"腰肌劳损"
    5727215	"9999804"	"2015-05-29 00:00:00.0"	"湿疹"
    5449077	"9999804"	"2015-08-03 00:00:00.0"	"腰椎间盘脱出"
    5315290	"9999804"	"2015-08-10 00:00:00.0"	"腰椎间盘脱出"
    */
    outclinical_words0616.take(10).foreach(println)
    /*原始门诊词库数据格式
    门诊诊断	ICD_CODE	Length
    镇癫癎药、镇静催眠药、抗帕金森病药和对精神有影响的药物的中毒及暴露于该类药物，不可归类在他处，意图不确定的	Y11	53
    器官的取除(部分)(全部)作为病人异常反应或以后并发症的原因，而在操作当时并未提及意外事故,其他的	Y83	49
    镇癫癎药、镇静催眠药、抗帕金森病药和对精神有影响的药物的故意自毒及暴露于该类药物，不可归类在他处者	X61	49
    镇癫癎药、镇静催眠药、抗帕金森病药和对精神有影响的药物的意外中毒及暴露于该类药物，不可归类在他处者	X41	49
    手术和医疗操作的后遗症作为病人异常反应或以后并发症的原因，而在操作当时并未提及意外事故	Y88	43
    胃或十二指肠探子的插入作为病人异常反应或以后并发症的原因，而在操作当时并未提及意外事故	Y84	43
    放射学操作和放射治疗作为病人异常反应或以后并发症的原因，而在操作当时并未提及意外事故	Y84	42
    副交感神经抑制剂[抗胆碱能药和抗毒蕈碱药]和解痉药的有害效应，其他的不可归类在他处者	Y51	42
    人工内部装置植入手术作为病人异常反应或以后并发症的原因，而在操作当时并未提及意外事故	Y83	42
    */

    // 2 构造DataFrame
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._//导入sqlContext所需要的包
    //将原始RDD转化为DataFrame
    val outclinical_diag2_df = outclinical_diag2.toDF()
    val outclinical_words0616_df = outclinical_words0616.toDF()
    //将DataFrame注册为临时表
    outclinical_diag2_df.registerTempTable("outclinical_diag")
    outclinical_words0616_df.registerTempTable("outclinical_words")

    outclinical_diag2_df.show()
    outclinical_words0616_df.show()












  }
}
