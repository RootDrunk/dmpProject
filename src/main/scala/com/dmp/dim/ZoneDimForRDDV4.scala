//package com.dmp.dim
//
////import com.dmp.bean.LogBean
////import com.dmp.utils.RedisUtil
////import org.apache.spark.SparkConf
////import org.apache.spark.rdd.RDD
////import org.apache.spark.sql.SparkSession
////import redis.clients.jedis.Jedis
////
////object ZoneDimForRDDV3 {
////    def main(args: Array[String]): Unit = {
////        if (args.length != 1) {
////            println(
////                """
////                  |缺少参数
////                  |inputpath
////                  |""".stripMargin)
////            sys.exit()
////        }
////
////        // 创建sparksession对象
////        var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
////
////        val spark = SparkSession.builder().config(conf).appName("ZoneDimForRDD").master("local[1]").getOrCreate()
////
////        var sc = spark.sparkContext
////
////        // 接收参数
////        var Array(inputPath) = args
////        //读取映射文件
////        val fileRdd: RDD[String] = sc.textFile(args(0))
////        //将使用逗号切割的数组封装到对象中变成rdd
////        val logBeanRdd: RDD[LogBean] = fileRdd.map(x => {
////            x.split("[,]", -1)
////        }).map(LogBean(_)).filter(x => x.appid.nonEmpty)
////
////        //映射方法 将空缺的appname补上，并且将数据格式映射成KV形式
////        val resultRdd: RDD[(String, List[Double])] = logBeanRdd.mapPartitions(ite => {
////            //读取Redis 一个分区开启一次链接
////            val jedis: Jedis = RedisUtil.getJedis
////            //将迭代器中的数据重新映射
////            val iterator: Iterator[(String, List[Double])] = ite.map(x => {
////                if (x.appname.equals("") || x.appname.isEmpty) {
////                    println(jedis.get(x.appid) + "--" + x.appid)
////                    if (jedis.get(x.appid) != null && jedis.get(x.appid).nonEmpty) {
////                        x.appname = jedis.get(x.appid)
////                    } else {
////                        x.appname = "未知appName"
////                    }
////                }
////                //将数据进行格式化
////                //调用判断方法
////                val qingqiu: List[Double] = ZoneDimJudge.qingQiuJudge(x.requestmode, x.processnode)
////                val canYu: List[Double] = ZoneDimJudge.canYuJingJiaJudge(x.adplatformproviderid, x.iseffective, x.isbilling, x.isbid, x.adorderid)
////                val chengGong: List[Double] = ZoneDimJudge.chengGongJingJiaJudge(x.adplatformproviderid, x.iseffective, x.isbilling, x.isbid, x.iswin)
////                val guangGao: List[Double] = ZoneDimJudge.guangGaoZhanShi(x.requestmode, x.iseffective)
////                val meiJie: List[Double] = ZoneDimJudge.meiJieZhanShi(x.requestmode, x.iseffective, x.isbilling)
////                val dsp: List[Double] = ZoneDimJudge.DSPXiaoFei(x.adplatformproviderid, x.iseffective, x.isbilling, x.iswin, x.adorderid, x.adcreativeid, x.winprice, x.adpayment)
////                (x.appname, qingqiu ++ canYu ++ chengGong ++ guangGao ++ meiJie ++ dsp)
////            })
////            jedis.close()
////            //返回这个迭代器
////            iterator
////        }).reduceByKey((x, y) => {
////            x.zip(y).map(x => {
////                x._1 + x._2
////            })
////        })
////        //输出到控制台
////        resultRdd.foreach(println)
////        //写出到磁盘的路径
////        resultRdd.saveAsTextFile(args(1))
////
////        println("程序运行结束")
////    }
////}
//
//
//
//
//
//
//
//
//
//
//
//
//import com.dmp.bean.LogBean
//import com.dmp.utils.RedisUtil
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}
//import redis.clients.jedis.Jedis
//
///**
// * 媒体多维度报表分析 从Redis中获取appname映射信息
// */
//object MediaDimForRedis {
//
//    def main(args: Array[String]): Unit = {
//        if (args.length != 2) {
//            println(
//                """
//                  |参数错误
//                  |""".stripMargin)
//            sys.exit()
//        }
//
//        //验证结束
//        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//
//        val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("ZoneDimByRdd").getOrCreate()
//
//        //使用spark对象读取parquet文件
//        val sc: SparkContext = spark.sparkContext
//
//
//        //判断磁盘是否存在文件 存在则删除
//        val system: FileSystem = FileSystem.get(sc.hadoopConfiguration)
//        if (system.exists(new Path(args(1)))) {
//            system.delete(new Path(args(1)), true)
//        }
//
//        //读取日志文件
//        val fileRdd: RDD[String] = sc.textFile(args(0))
//        //将使用逗号切割的数组封装到对象中 变成rdd
//        val logBeanRdd: RDD[LogBean] = fileRdd.map(x => {
//            x.split("[,]", -1)
//        }).map(LogBean(_)).filter(x => x.appid.nonEmpty)
//
//
//        //映射方法 将空缺的appname补上，并且将数据格式映射成KV形式
//        val resultRdd: RDD[(String, List[Double])] = logBeanRdd.mapPartitions(ite => {
//            //读取Redis 一个分区开启一次链接
//            val jedis: Jedis = RedisUtil.getJedis
//            //将迭代器中的数据重新映射
//            val iterator: Iterator[(String, List[Double])] = ite.map(x => {
//                if (x.appname.equals("") || x.appname.isEmpty) {
//                    println(jedis.get(x.appid) + "--" + x.appid)
//                    if (jedis.get(x.appid) != null && jedis.get(x.appid).nonEmpty) {
//                        x.appname = jedis.get(x.appid)
//                    } else {
//                        x.appname = "未知appName"
//                    }
//                }
//                //将数据进行格式化
//                //调用判断方法
//                val qingqiu: List[Double] = DIMZhibiao.qqsRtp(x.requestmode, x.processnode)
//                val canYu: List[Double] = DIMZhibiao.jingjiaRtp(x.adplatformproviderid, x.iseffective, x.isbilling, x.isbid, x.adorderid)
//                val chengGong: List[Double] = DIMZhibiao.chengGongJingJiaJudge(x.adplatformproviderid, x.iseffective, x.isbilling, x.isbid, x.iswin)
//                val guangGao: List[Double] = DIMZhibiao.guangGaoZhanShi(x.requestmode, x.iseffective)
//                val meiJie: List[Double] = DIMZhibiao.meiJieZhanShi(x.requestmode, x.iseffective, x.isbilling)
//                val dsp: List[Double] = DIMZhibiao.DSPXiaoFei(x.adplatformproviderid, x.iseffective, x.isbilling, x.iswin, x.adorderid, x.adcreativeid, x.winprice, x.adpayment)
//                (x.appname, qingqiu ++ canYu ++ chengGong ++ guangGao ++ meiJie ++ dsp)
//            })
//            jedis.close()
//            //返回这个迭代器
//            iterator
//        }).reduceByKey((x, y) => {
//            x.zip(y).map(x => {
//                x._1 + x._2
//            })
//        })
//        //输出到控制台
//        resultRdd.foreach(println)
//        //写出到磁盘的路径
//        resultRdd.saveAsTextFile(args(1))
//
//        println("程序运行结束")
//
//    }
//
//}
