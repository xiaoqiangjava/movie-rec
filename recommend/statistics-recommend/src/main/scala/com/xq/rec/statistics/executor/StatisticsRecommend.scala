package com.xq.rec.statistics.executor

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.xq.rec.statistics.constant.Constant
import com.xq.rec.statistics.model.{Movie, MysqlConf, Rating}
import com.xq.rec.statistics.utils.StorageUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * 离线统计推荐：
  *     历史热门电影统计
  *     最近热门电影统计
  *     电影平均得分统计
  *     每个类别优质电影推荐(topN)
  */
object StatisticsRecommend {
    val logger = LoggerFactory.getLogger(StatisticsRecommend.getClass)
    val conf = Map("spark.master" -> "local[2]",
        "mongo.uri" -> "mongodb://localhost:27017/recommend",
        "mongo.db" -> "recommend",
        "mysql.url" -> "jdbc:mysql://localhost:3306/recommend",
        "user" -> "root",
        "password" -> "root")

    def main(args: Array[String]): Unit = {
        // 创建sparkSession
        val sparkConf = new SparkConf()
        sparkConf.setAppName(StatisticsRecommend.getClass.getSimpleName)
            .setMaster(conf("spark.master"))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 从mysql中加载数据
        val props = new Properties()
        props.setProperty("user", conf("user"))
        props.setProperty("password", conf("password"))
        import spark.implicits._
        val ratingDF = spark.read.jdbc(conf("mysql.url"), Constant.RATING_COLLECTION, props).as[Rating].toDF()
        val movieDF = spark.read.jdbc(conf("mysql.url"), Constant.MOVIE_COLLECTION, props).as[Movie].toDF()
        // 创建临时表
        ratingDF.createOrReplaceTempView("rating")
        ratingDF.cache()

        // 1. 历史热门推荐，即历史评分数据最多，mid, count
        val historyHotDF = spark.sql("select mid, count(mid) as count from rating group by mid order by count desc")
        logger.info("History hot rec.")
        historyHotDF.show()
        implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
        StorageUtil.storeResult2Mysql(historyHotDF, Constant.HISTORY_HOT_REC)

        // 2. 近期热门推荐，按照yyyyMM格式选取最近的评分数据，统计评分个数
        val sdf = new SimpleDateFormat("yyyyMM")
        // 自定义函数udf使用场景：如果在spark.sql()中的SQL语句中使用，则要使用spark.udf.register()来注册，要是在DataFrame中的select()中使用，可以使用udf()函数来注册
//        import org.apache.spark.sql.functions._
//        val formatDate = udf((timestamp: Int) => sdf.format(new Date(timestamp * 1000L)).toInt)
        spark.udf.register("formatDate", (timestamp: Int) => sdf.format(new Date(timestamp * 1000L)).toInt)
        // 自定义函数跟聚合函数一起使用时会报错，需要先将自定义函数得到的列封装成一张表
        val recentTableDF = spark.sql("select mid, formatDate(timestamp) as yearmonth from rating")
        recentTableDF.createOrReplaceTempView("recent")
        recentTableDF.show()
        val recentDF = spark.sql("select mid, yearmonth, count(mid) as count from recent group by yearmonth, mid order by yearmonth desc, count desc")
        logger.info("Recent hot rec...")
        recentDF.show()
        StorageUtil.storeResult2Mysql(recentDF, Constant.RECENT_HOT_REC)
        // 3. 优质电影推荐，统计电影的平均得分
        val avgDF = spark.sql("select mid, avg(score) as score from rating group by mid order by score desc")
        logger.info("High score movie rec...")
        avgDF.show()
        StorageUtil.storeResult2Mysql(avgDF, Constant.AVERAGE_SCORE_REC)
        // 4. 各类别电影topN统计
    }
}
