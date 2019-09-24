package Tag

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import util.TagsUtils

object TagsContext2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()
    // 读取字典文件
    val docsRDD = spark.sparkContext.textFile("zidian.txt").map(_.split("\\s")).filter(_.length>=5)
      .map(arr=>(arr(4),arr(1))).collectAsMap()
    // 广播字典
    val broadValue = spark.sparkContext.broadcast(docsRDD)
    // 读取停用词典
    val stopwordsRDD = spark.sparkContext.textFile("word.txt").map((_,0)).collectAsMap()
    // 广播字典
    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)

    val df = spark.read.parquet("B.parquent")
    val allUerID =df.rdd.map(row=>{
      //获取所有ID
      val str = TagsUtils.getallUserId(row)
      (str,row)
    })
    //创建点jihe
    val verties = allUerID.flatMap(row=>{
      val rows =row._2

      val asList = TagsAd.mkTags(rows)
      // 商圈
      val business = BusinessTag.mkTags(rows,broadValue)
      //设备标签
      val devList = TafsDevice.mkTags(rows)
      // 地域标签
      val locList =TagsLocaltion.mkTags(rows)
      // 地域标签
      val kwList = TagsKword.mkTags(rows,broadValues)

      val tagList = asList ++ business++devList++locList++kwList
      //保留用户ID
      val vd = row._1.map((_,1))++tagList
      row._1.map(uId=>{
        if(row._1.head.equals(uId)){
          (uId.hashCode.toLong,vd)
        }else{
          (uId.hashCode.toLong,List.empty)
        }
      })
    })
    //创建bian集和
    val edges =allUerID.flatMap(row=>{
      row._1.map(uId=>{
        Edge(row._1.head.hashCode.toLong,uId.hashCode.toLong,0)
      })
    })

    //创建图
    val graph =Graph(verties,edges)
    // 根据图计算中的连通图算法，通过图中的分支，连通所有的点
    // 然后在根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices
    vertices.join(verties).map {
      case (uid, (cnId, tagsAndUserId)) => {
        (cnId, tagsAndUserId)
      }
    }.reduceByKey(
      (list1,list2)=>{
        (list1++list2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
      })
    spark.stop()
  }
}

