package com.dmp.tools

import com.dmp.bean.LogBean
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Log2Parquet2 {
    def main(args: Array[String]): Unit = {
        // 判断参数是否正确。
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }
        //创建sparksession对象
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //将自定义对象进行kryo序列化
        conf.registerKryoClasses(Array(classOf[LogBean]))

        val spark: SparkSession = SparkSession.builder().config(conf).appName("Log2Parquet2").master("local[*]").getOrCreate()
        val sc: SparkContext = spark.sparkContext
        import spark.implicits._

        //接收参数
        var Array(inputPath, outputPath) = args

        val rdd: RDD[Array[String]] = sc.textFile(inputPath).map(_.split(",", -1)).filter(_.length >= 85)

    val rddLogBean: RDD[LogBean] = rdd.map(LogBean(_))

    val df: DataFrame = spark.createDataFrame(rddLogBean)

    df.write.parquet(outputPath)

    spark.stop()
    sc.stop()

    }
}
