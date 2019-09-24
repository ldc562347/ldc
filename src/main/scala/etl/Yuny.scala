package etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RptUtils

object Yuny {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()
    import  spark.implicits._
    val docMap: DataFrame =spark.read.parquet("B.parquent")
    val res= docMap.map((row)=>{
      val requestmode =  row.getAs[Int]("requestmode")
      val processnode =  row.getAs[Int]("processnode")
      val iseffetive =  row.getAs[Int]("iseffective")
      val isbilling =  row.getAs[Int]("isbilling")
      val isbid =  row.getAs[Int]("isbid")
      val iswin =  row.getAs[Int]("iswin")
      val adordeerid =  row.getAs[Int]("adorderid")
      val winprice =  row.getAs[Double]("winprice")
      val adpayment =  row.getAs[Double]("adpayment")
      val reqpty = RptUtils.Reqpt(requestmode,processnode)
      val clickPty = RptUtils.clickPt(requestmode:Int,iseffetive:Int)
      val abPty = RptUtils.adPt(iseffetive:Int,isbilling:Int,
        isbid:Int,iswin:Int,adordeerid:Int,winprice:Double
        ,adpayment:Double)
      val alllist = reqpty++clickPty++abPty
      (row.getAs[String]("ispname"),alllist)
    }).rdd.reduceByKey((list1,list2)=>{
      val list3 = list1.zip(list2).map(t=>t._1+t._2)
      list3
    }).map(t=>t._1+""+t._2)
    res.saveAsTextFile("y.txt")
    spark.stop()
  }
}
