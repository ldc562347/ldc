package etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RptUtils

object Cz {
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
      // 设备类型 （1：android 2：ios 3：wp）
      var type2=""
      var type1 =row.getAs[Int]("client")
      if(type1==1){
        type2="android"
      }else if(type1==2){
        type2="ios"
      }else if(type1==3){
        type2="wp"
      }else{
        type2="其他"
      }

      (type2,alllist)

    }).rdd.reduceByKey((list1,list2)=>{
      val list3 = list1.zip(list2).map(t=>t._1+t._2)
      list3
    })//.map(t=>t._1+","+t._2)
    res.saveAsTextFile("cz.txt")

    spark.stop()
  }
}
