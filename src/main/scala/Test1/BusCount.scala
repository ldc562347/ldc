package Test1

import org.apache.spark.sql.{Dataset, SparkSession}

class BusCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Ct")
      .master("local")
      .getOrCreate()
    val ds: Dataset[String] = spark.read.textFile("json.txt")
    ds.rdd.map(line=>{
      line.split("")
    })
  }
}
