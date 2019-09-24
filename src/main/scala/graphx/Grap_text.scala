package graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object Grap_text {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()
    val vertexRDD = spark.sparkContext.makeRDD(
      Seq(
        (1L,("小明",26)),
        (2L,("小红",30)),
        (6L,("小黑",33)),
        (9L,("小白",26)),
        (133L,("小黄",30)),
        (138L,("小蓝",33)),
        (158L,("小绿",26)),
        (16L,("小龙",30)),
        (44L,("小强",33)),
        (21L,("小胡",26)),
        (5L,("小狗",30)),
        (7L,("小熊",33))
      )
    )
    //构建边集合
    val edge = spark.sparkContext.makeRDD(
      Seq(
        Edge(1L,133L,0),
        Edge(2L,133L,0),
        Edge(6L,133L,0),
        Edge(9L,133L,0),
        Edge(6L,138L,0),
        Edge(16L,138L,0),
        Edge(21L,138L,0),
        Edge(44L,138L,0),
        Edge(5L,158L,0),
        Edge(7L,158L,0)
      )
    )
    //构建图
    val grap = Graph(vertexRDD,edge)
    //取顶点
    val vertice = grap.connectedComponents().vertices
    //匹配数据
    vertice.join(vertexRDD).map{
      case (userId,(cnID,(name,age)))=>(cnID,List((name,age))
      )
    }.reduceByKey(_++_)
      .foreach(println)
  }
}
