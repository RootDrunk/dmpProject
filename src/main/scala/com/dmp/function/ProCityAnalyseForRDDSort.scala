package com.dmp.function

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object ProCityAnalyseForRDDSort {
    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            println(
                """
				  |缺少参数
				  |inputPath outputPath
				  |""".stripMargin)
            sys.exit(0)
        }


        // 创建 SparkSession对象
        val conf: SparkConf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


        val spark: SparkSession = SparkSession
                .builder
                .config(conf)
//                .appName("ProCityAnalyseForRDDSort")
//                .master("local[1]")
                .getOrCreate


        val sc: SparkContext = spark.sparkContext

        val line: RDD[String] = sc.textFile(args(0))

        val fieid: RDD[Array[String]] = line.map(_.split(",", -1))


        val proCityRDD: RDD[((String, String), Int)] = fieid.filter(_.length >= 85).map(
            line => {
                ((line(24), line(25)), 1)
            }
        )

        val reduceRDD: RDD[((String, String), Int)] = proCityRDD.reduceByKey(_ + _)

        reduceRDD.cache()


        val index: Int = reduceRDD.map(
            _._1._1

        ).distinct().count().toInt

        reduceRDD.sortBy(_._2).coalesce(1).partitionBy(new MyPartition(index))
                .saveAsTextFile(args(1))


        sc.stop
        spark.close
    }

    class MyPartition(val count: Int) extends Partitioner {
        override def numPartitions: Int = count

        private var index = -1
        private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()

        override def getPartition(key: Any): Int = {
            val value: String = key.toString
            val str: String = value.substring(1, value.indexOf(","))
            println(str)
            if (map.contains(str)) {
                map.getOrElse(str, index)
            } else {
                index += 1
                map.put(str, index)
                index
            }
        }
    }
}
