package util

import com.alibaba.fastjson.{JSON, JSONObject}

object AmapUtil {
  def getBusinessFromAmap(long:Double,lat:Double):String= {
    // https://restapi.amap.com/v3/geocode/regeo?
    // location=116.310003,39.991957&key=59283c76b065e4ee401c2b8a4fde8f8b&extensions=all
    val location = long + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&key=59283c76b065e4ee401c2b8a4fde8f8b"
    //调用Http接口发送请求
    val jsonstr = HttpUtil.get(url)
    //解析json
    val JSONObject1 = JSON.parseObject(jsonstr)
    val statue = JSONObject1.getIntValue("status")
    if (statue == 0) return ""
    //不为空
    val JSONObject = JSONObject1.getJSONObject("regeocode")
    if (JSONObject == null) return ""
    val JSONObject2 = JSONObject.getJSONObject("addressComponent")
    if (JSONObject2 == null) return ""
    val JSONArray = JSONObject2.getJSONArray("businessAreas")
    if (JSONArray == null) return ""
    val result = collection.mutable.ListBuffer[String]()
    for (item <- JSONArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }
}
