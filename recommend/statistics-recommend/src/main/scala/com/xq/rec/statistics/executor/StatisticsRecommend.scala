package com.xq.rec.statistics.executor

import java.util.Properties

import com.xq.rec.statistics.constant.Constant
import com.xq.rec.statistics.model.{Movie, Rating}
import com.xq.rec.statistics.offline.{AverageScoreRec, ClassicTopNRec, HistoryHotRec, RecentHotRec}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * 离线统计推荐：
  *     历史热门电影统计：历史评分数据最多的电影
  *     最近热门电影统计：最近评分数据最多的电影
  *     电影平均得分统计：用户对一个电影的平均得分
  *     每个类别优质电影推荐(topN)： 每个类别中电影平均得分topN
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
        movieDF.cache()
        movieDF.show()

        // 1. 历史热门推荐，即历史评分数据最多，mid, count
        val historyHotRec = new HistoryHotRec(conf)
        historyHotRec.fit(spark)

        // 2. 近期热门推荐，按照yyyyMM格式选取最近的评分数据，统计评分个数
        val recentHotRec = new RecentHotRec(conf)
        recentHotRec.fit(spark)

        // 3. 优质电影推荐，统计电影的平均得分
        val averageScoreRec = new AverageScoreRec(conf)
        val avgScoreDF = averageScoreRec.fit(spark)
        avgScoreDF.cache()

        // 4. 各类别电影topN统计
        val classicTopNRec = new ClassicTopNRec(conf)
        classicTopNRec.fit(spark, movieDF, avgScoreDF)

        spark.close()
    }
}
