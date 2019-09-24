package location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
//统计省市指标
object ProCityCt {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop01", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")

    if(args.length != 1 ){
      println("目录不正确")
      sys.exit()
    }
    val  Array(inputPath) = args
    val spark = SparkSession
      .builder()
      .appName("Ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
 //获取数据
    val df = spark.read.parquet(inputPath)
    //注册临时视图
    df.createTempView("table_ct")
    val df2 = spark.sql("select provincename,cityname,count(*) ct from table_ct group by provincename,cityname")
    //直接存json
   // df2.write.partitionBy("provincename,cityname").json("D:\\pro")
    //存储mysql
    //通过config配置文件进行加载信息
    val load = ConfigFactory.load()
    //创建properties对象
    val pop = new Properties()
    pop.setProperty("user",load.getString("jdbc.user"))
    pop.setProperty("password",load.getString("jdbc.password"))
    df2.write.mode(SaveMode.Append)
      .jdbc(load.getString("jdbc.url"),load.getString("jdbc.tablename"),pop)
    spark.stop()
  }
}
