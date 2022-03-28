package com.dmp.tag

import org.apache.spark.sql.Row

object KeyTags extends TagTrait {
    override def makeTags(args: Any*): Map[String, Int] = {
        var map = Map[String,Int]()
        val row: Row = args(0).asInstanceOf[Row]

        //停用词
        val broadcast: Map[String, Int] = args(1).asInstanceOf[Map[String, Int]]

        val keywords: String = row.getAs[String]("keywords")
        keywords.split("\\|")
                .filter(kw => kw.length >= 3 && kw.length <= 8 && !broadcast.contains(kw))
                .foreach(kw => map += "K" + kw -> 1)
        map
    }
}
