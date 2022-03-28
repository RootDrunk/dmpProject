package com.dmp.tag

import com.dmp.tools.SNTools
import com.dmp.tools.SNTools.getBusiness
import com.dmp.utils.TagUtil
import java.util.UUID
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TagRpt {
    def main(args: Array[String]): Unit = {
        if (args.length != 4) {
            println(
                """
				  |缺少参数
				  |inputpath,appMapping,stopwerds  outputpath
				  |""".stripMargin)
            sys.exit(0)
        }
        // 创建 SparkSession对象
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val spark: SparkSession = SparkSession.builder.config(conf).appName("ZoneDim").master("local[1]").getOrCreate

        val sc: SparkContext = spark.sparkContext
        import spark.implicits._
        val Array(inputPath, app_Mapping, stopWerds, outputPath): Array[String] = args

        //读取app_Mapping广播变量
        val app_map: Map[String, String] = sc.textFile(app_Mapping).map(line => {
            val strs: Array[String] = line.split("[:]", -1)
            (strs(0), strs(1))
        }).collect().toMap

        //广播app变量
        val broadCastAppMap: Broadcast[Map[String, String]] = sc.broadcast(app_map)

        val stopWordMap: Map[String, Int] = sc.textFile(stopWerds).map((_, 0)).collect().toMap
        val broadcastStopWord: Broadcast[Map[String, Int]] = sc.broadcast(stopWordMap)

        //读取数据源，进行标签
        val df: DataFrame = spark.read.parquet(inputPath)

        val TagDS: Dataset[(String, List[(String, Int)])] = df.where(TagUtil.tagUserIdFilterParam).map(row => {
            //广告标签
            val adsMap: Map[String, Int] = AdsTags.makeTags(row)
            //app标签
            val appMap: Map[String, Int] = AppTags.makeTags(row, broadCastAppMap.value)
            //驱动标签
            val driverMap: Map[String, Int] = DriverTags.makeTags(row)
            //关键字标签
            val keyMap: Map[String, Int] = KeyTags.makeTags(row, broadcastStopWord.value)
            //地域标签
            val pcMap: Map[String, Int] = PcTags.makeTags(row)
            //商圈标签
            val LOA: Map[String, Int] = Longitude.makeTags(row)

            //获取用户ID
            //(TagUtil.getUserId(row)(0),(adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap ).toList)
            if (TagUtil.getUserId(row).size > 0) {
                (TagUtil.getUserId(row)(0) , (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap  ++LOA).toList)
            } else {
                (UUID.randomUUID().toString.substring(0, 6), (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap  ++LOA).toList)
            }
        })
        TagDS.rdd.reduceByKey((list1, list2) => {
            (list1 ++ list2).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
        }).saveAsTextFile(outputPath)
    }
}


