package Tag

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import util.TagsUtils

object TagsContext {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()
    //调用HbaseAPI
    val load = ConfigFactory.load()
    //获取表名
    // HBASE.Host="node1:2181"
    // HBASE.tableName
    val hbasetable = load.getString("HBASE.tableName")
    //创建Hadoop任务
    val confiugration = spark.sparkContext.hadoopConfiguration
    //配置Hbase连接
    confiugration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
    //获取connection连接
    val hbConn = ConnectionFactory.createConnection(confiugration)
    val hbadmin =hbConn.getAdmin
    //判断当前表是否被使用
    if(!hbadmin.tableExists(TableName.valueOf(hbasetable))){
      println("当前表可用")
      //创建表对象
      val tableDescriptors = new HTableDescriptor(TableName.valueOf(hbasetable))
      //创建列簇
      val HColumnDescriptor = new HColumnDescriptor("tags")
      //将列簇加入表中
      tableDescriptors.addFamily(HColumnDescriptor)
      hbadmin.createTable(tableDescriptors)
      hbadmin.close()
      hbConn.close()
    }
    val conf = new JobConf(confiugration)
    //指定输出类型
    conf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出de表
    conf.set(TableOutputFormat.OUTPUT_TABLE,hbasetable)
    val context = spark.sparkContext.textFile("zidian.txt").map(_.split("\\s"))
      .filter(_.length>=5).map(arr=>(arr(1),arr(4)))
      .collectAsMap()
    val broadvalue = spark.sparkContext.broadcast(context)
    val df = spark.read.parquet(  "B.parquent")
    df.rdd.map((row)=>{
      val list = TagsAd.mkTags(row)
      val list2 = TagsUtils.getOneUserId(row)
      //媒体标签
      val appLis = TagsApp.mkTags(row,broadvalue)
      (list2,list++appLis)
    })
    /*.reduceByKey((list1,list2)=>{
      (list1:::list2).groupBy(_._1)
        .mapValues(_.foldLeft(0)(_+_._2))
        .toList
    }).map{
      case (userID,userTags)=>{
        //设置rowkey和列，列明
        val put= new Put(Bytes.toBytes(userID))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("20190922"),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(conf)*/

  }
}
