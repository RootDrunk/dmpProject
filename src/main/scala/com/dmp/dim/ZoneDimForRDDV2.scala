package com.dmp.dim

import com.dmp.bean.LogBean
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.glassfish.jersey.server.Broadcaster

object ZoneDimForRDDV2 {
    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            println(
                """
                  |缺少参数
                  |inputpath appMapping outputpath
                  |""".stripMargin)
            sys.exit()
        }

        // 创建sparksession对象
        var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val spark = SparkSession.builder().config(conf).appName("ZoneDimForRDD").master("local[1]").getOrCreate()

        var sc = spark.sparkContext

        import spark.implicits._

        // 接收参数
        var Array(inputPath, appMapping ,outputPath) = args
        //读取映射文件：appMapping
        val appMap: Map[String, String] = sc.textFile(appMapping).map(line => {
            val arr: Array[String] = line.split("[:]", -1)
            (arr(0), arr(1))
        }).collect().toMap

        //使用广播变量，进行广播
        val appBroadcast: Broadcast[Map[String, String]] = sc.broadcast(appMap)

        val log: RDD[String] = sc.textFile(inputPath)
        val logRDD: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter({t => {
             !t.appid.isEmpty
        }

        })
        logRDD.map(log => {
            val appname: String = log.appname
            if(appname == "" || appname.isEmpty){
                appBroadcast.value.getOrElse(log.appid,"不明确")
            }
            val ysqqs: List[Double] = DIMZhibiao.qqsRtp(log.requestmode, log.processnode)
            (appname,ysqqs)

        }).reduceByKey((list1, list2) => {
            list1.zip(list2).map(t => t._1 + t._2)
        }).foreach(println)
    }
}
