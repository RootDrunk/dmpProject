package com.dmp.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object PcTags extends TagTrait {
    override def makeTags(args: Any*): Map[String, Int] = {
        var map = Map[String,Int]()
        val row: Row = args(0).asInstanceOf[Row]
        val prowinceName: String = row.getAs[String]("provincename")
        val cityName: String = row.getAs[String]("cityname")
        if(StringUtils.isNotEmpty("prowincename")) map += "ZP" + prowinceName -> 1
        if(StringUtils.isNotEmpty("cityName")) map += "ZP" + cityName -> 1
        map
    }
}
