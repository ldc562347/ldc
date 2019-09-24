package Tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import util.Tag

object TagsApp extends Tag{
  override def mkTags(args: Any*): List[(String, Int)] ={
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val broad = args(1).asInstanceOf[Broadcast[collection.Map[String,Int]]]
    val appname =row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else{
      list:+=("APP"+broad.value.getOrElse(appid,"其他"),1)
    }
    list
  }
}
