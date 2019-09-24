package location

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import util.RptUtils

//媒体分析指标
class APP{

}
object APP {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop01", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")

    if(args.length != 3 ){
      println("目录不正确")
      sys.exit()
    }
    val  Array(inputPath,outputPath,docs) = args
    val spark = SparkSession
      .builder()
      .appName("Ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //读取数据字典
    val docMap = spark.sparkContext.textFile(docs).map(_.split("\\s",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    //进行广播
    val brodcast = spark.sparkContext.broadcast(docMap)
    //读取数据文件
    val df = spark.read.parquet(inputPath)
    df.rdd.map(row=>{
      //取媒体相关字段
      var appName =row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = brodcast.value.getOrElse(row.getAs("appid"),"unknow")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //梳理请求书
      val reqptl = RptUtils.Reqpt(requestmode,processnode)
      //处理展示点击
      val clickl = RptUtils.clickPt(requestmode,iseffective)
      //处理广告
      val adptl =  RptUtils.adpt(iseffective,isbilling,isbid,
        iswin,adordeerid,winprice,adpayment)
      //所有指标
      val allList:List[Double] = reqptl ++ clickl ++ adptl
    })

  }
}
