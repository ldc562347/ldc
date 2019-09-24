package util


import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark =  SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read.parquet("B.parquent")
    df.rdd.map(row=>{
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat"))
      )
    }).foreach(println)
  }
}