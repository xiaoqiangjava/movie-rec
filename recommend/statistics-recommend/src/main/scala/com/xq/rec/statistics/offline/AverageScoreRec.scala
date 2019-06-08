package com.xq.rec.statistics.offline

import com.xq.rec.statistics.constant.Constant
import com.xq.rec.statistics.executor.StatisticsRecommend.logger
import com.xq.rec.statistics.model.MysqlConf
import com.xq.rec.statistics.utils.StorageUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于平均评分的推荐
  */
class AverageScoreRec(conf: Map[String, String]) {

    /**
      * 统计平均得分较高的电影
      * @param spark sparkSession
      */
    def fit(spark: SparkSession): DataFrame = {
        val avgDF = spark.sql("select mid, avg(score) as score from rating group by mid order by score desc")
        logger.info("High score movie rec...")
        avgDF.show()
        implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
        StorageUtil.storeResult2Mysql(avgDF, Constant.AVERAGE_SCORE_REC)
        return avgDF
    }
}
