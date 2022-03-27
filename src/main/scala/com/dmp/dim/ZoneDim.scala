package com.dmp.dim

import com.typesafe.config.{Config, ConfigFactory}
import java.util.Properties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ZoneDim {
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
        val spark: SparkSession = SparkSession.builder.config(conf).appName("ZoneDim").master("local[1]").getOrCreate

        val sc: SparkContext = spark.sparkContext
        import spark.implicits._
        val Array(inputPath, outputPath): Array[String] = args

        val df: DataFrame = spark.read.parquet(inputPath)

        //创建表
        val dim: Unit = df.createTempView("dim")

        //sql语句
        var sql =
            """
              |select
              |provincename,cityname,
              |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysqq,
              |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as yxqq,
              |sum(case when requestmode = 1 and processnode >= 3 then 1 else 0 end) as ggqq,
              |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0 then 1 else 0 end) as cyjqq,
              |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1  then 1 else 0 end) as jjcgqq,
              |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as zsqq,
              |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as djqq,
              |sum(case when requestmode = 2 and iseffective = 1 and isbilling = 1 then 1 else 0 end) as zsmqq,
              |sum(case when requestmode = 3 and iseffective = 1 and isbilling = 1 then 1 else 0 end) as djmqq,
              |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid > 200000 and adcreativeid > 200000 then (adpayment*1.0)/1000 else 0 end) as xiaofei,
              |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid > 200000 and adcreativeid > 200000 then (adpayment*1.0)/1000 else 0 end) as chengben
              |from dim
              |group by
              |provincename,cityname
              |""".stripMargin

        val resDF: DataFrame = spark.sql(sql)

        //输出到mysql
        val load: Config = ConfigFactory.load()
        val properties = new Properties()
        properties.setProperty("user", load.getString("jdbc.user"))
        properties.setProperty("driver", load.getString("jdbc.driver"))
        properties.setProperty("password", load.getString("jdbc.password"))


        resDF.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), load.getString("jdbc.tableName2"), properties)

        spark.stop()
        sc.stop()
    }
}
