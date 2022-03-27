package com.dmp.dim

import com.dmp.bean.LogBean
import com.dmp.utils.RedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object ZoneDimForRDDV3 {
    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            println(
                """
                  |缺少参数
                  |inputpath  outputpath
                  |""".stripMargin)
            sys.exit()
        }

        // 创建sparksession对象
        var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val spark = SparkSession.builder().config(conf).appName("ZoneDimForRDD").master("local[1]").getOrCreate()

        var sc = spark.sparkContext

        // 接收参数
        var Array(inputPath ,outputPath) = args
        sc.textFile(inputPath)
                .map(_.split("[,]",-1))
                .filter(_.length >= 85)
                .map(LogBean(_))
                .filter(t => {
                    t.appid.nonEmpty
                })
                .mapPartitions(its =>{
                    val appName = ""
                    val nil: List[Double] = Nil
                    val redis: Jedis = RedisUtil.getJedis
                    val tuples: Iterator[(String, List[Double])] = its.map(x => {
                        val appname: String = x.appname
                        if (appname == "" || appname.isEmpty) {
                            val appnames: String = redis.get(x.appid)
                        }
                        val ysqqs: List[Double] = DIMZhibiao.qqsRtp(x.requestmode, x.processnode)
                        (appname, ysqqs)
                    })
                    redis.close()
                    tuples
                })
                .reduceByKey((list1,list2) => {
                    list1.zip(list2)
                            .map( tup => {
                                tup._1 + tup._2
                            })
                })
                .foreach(println)
        sc.stop()
        spark.stop()
    }
}
