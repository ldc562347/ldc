package Tag

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.{AmapUtil, JedisConnecttionPool, String2Type, Tag}

object BusinessTag extends Tag {

  def redis_queryBusiness(geohash: String): String = {
    val jedis = JedisConnecttionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  def redis_insertBusiness(geohash: String, business: String): Unit = {
    val jedis = JedisConnecttionPool.getConnection()
    jedis.set(geohash, business)
    jedis.close()
  }


  def getBusiness(long: Double, lat: Double): String = {
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 6)
    var business = redis_queryBusiness(geohash)
    if (business == null) {
      business = AmapUtil.getBusinessFromAmap(long, lat)
      if (business != null && business.length > 0) {
        redis_insertBusiness(geohash, business)
      }
    }
    business


  }

  override def mkTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args.asInstanceOf[Row]
    if (String2Type.toDouble(row.getAs[String]("long")) >= 73
      && String2Type.toDouble(row.getAs[String]("long")) <= 136
      && String2Type.toDouble(row.getAs[String]("lat")) >= 3
      && String2Type.toDouble(row.getAs[String]("lat")) <= 53) {
      // 经纬度
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
      // 获取到商圈名称
      val business = getBusiness(long, lat)
      if (StringUtils.isNoneBlank(business)) {
        val str = business.split(",")
        str.foreach(str => {
          list :+= (str, 1)
        })
      }
    }
    list
  }
}
