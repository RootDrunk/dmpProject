package com.dmp.function

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}


object ProCityAnalyse {
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
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val spark: SparkSession = SparkSession.builder.config(conf).appName("Log2Parquet").master("local[1]").getOrCreate

        val sc: SparkContext = spark.sparkContext
        import spark.implicits._
        val Array(inputPath, outputPath): Array[String] = args
        val df: DataFrame = spark.read.parquet(inputPath)
        df.createTempView("log")
        //编写sql语句
        val sql: String =
        """
			  |select
			  |	 provincename province,
			  |  cityname city,
			  |  count(1) as pcCnt
			  |from log
			  |group by provincename, cityname
			  |""".stripMargin

        val resDF: DataFrame = spark.sql(sql)
        val configuration: Configuration = sc.hadoopConfiguration
        //文件系统对象
        val fs: FileSystem = FileSystem.get(configuration)
        val path = new Path(outputPath)
        if (fs.exists(path)) fs.delete(path, true)
        else resDF.coalesce(1).write.partitionBy("province", "city").json(outputPath)
        sc.stop
        spark.close
    }
}
