package com.dmp.tag

import com.dmp.tools.SNTools
import org.apache.spark.sql.Row

object Longitude extends TagTrait {
    override def makeTags(args: Any*): Map[String, Int] = {
        var map = Map[String,Int]()
        val row: Row = args(0).asInstanceOf[Row]
        //经度
        val Longs: String = row.getAs[String]("long")
        //纬度
        val Lat: String = row.getAs[String]("lat")

        val sq: String = SNTools.getBusiness( Lat + "," + Longs )
//        println(sq)
        if(sq != ""){
            println("纬度:"+Lat+"\t"+"经度:"+Longs)
            println("商圈：" + sq)
            map += sq -> 1
        }else{
            println("纬度:"+Lat+"\t"+"经度:"+Longs)
            println("商圈：无")
        }
       map
    }
}
