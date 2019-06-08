package com.xq.rec.statistics.offline

import com.xq.rec.statistics.constant.Constant
import com.xq.rec.statistics.model.{GenresRecommendation, MysqlConf, Predict}
import com.xq.rec.statistics.utils.StorageUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 基于电影类别的topN推荐
  */
class ClassicTopNRec(conf: Map[String, String]) {
    val logger = LoggerFactory.getLogger(classOf[ClassicTopNRec])
    /**
      * 按照电影类别推荐出该分类中平均得分高的电影列表
      * @param spark sparkSession
      * @param movieDF movieDF
      * @param avgScoreDF avgScoreDF
      */
    def fit(spark: SparkSession, movieDF: DataFrame, avgScoreDF: DataFrame): Unit = {
        // 从电影数据中获取电影分类信息genres, Action|Adventure|Comedy
        import spark.implicits._
        import org.apache.spark.sql.functions._
        val genresArray = movieDF.select("genres")
            .agg(concat_ws("|", collect_set("genres")))
            .map(row => row.getString(0).split("\\|").map(_.trim).distinct)
            .first()
        // 将电影分类信息转成RDD，与电影评分数据做笛卡尔积
        val genresRDD = spark.sparkContext.makeRDD(genresArray)

        logger.info("genres: {}", genresRDD.take(20))
        // 把电影数据跟电影评分数据加入到一张表里, 需要统计有评分数据的电影，所以使用内连接
        val movieScoreDF = movieDF.join(avgScoreDF, "mid")
        val genresTopN = genresRDD.cartesian(movieScoreDF.rdd).filter({
            case (genres, item) => item.getAs[String]("genres").contains(genres)
        }).map({
            case (genres, item) => (genres, (item.getAs[Int]("mid"), item.getAs[Double]("score")))
        }).groupByKey().map({
            case (genres, predictList) => GenresRecommendation(genres, predictList.toList.sortWith(_._2 > _._2).take(10).mkString("|"))
        }).toDF()
        logger.info("genresTopN: {}", genresTopN.take(20))
        genresTopN.show()
        implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
        StorageUtil.storeResult2Mysql(genresTopN, Constant.GENRES_TOP_REC)
    }
}
