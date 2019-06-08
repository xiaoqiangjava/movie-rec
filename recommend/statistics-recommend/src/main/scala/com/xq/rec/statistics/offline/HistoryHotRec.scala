package com.xq.rec.statistics.offline

import com.xq.rec.statistics.constant.Constant
import com.xq.rec.statistics.model.MysqlConf
import com.xq.rec.statistics.utils.StorageUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * 历史热门推荐
  */
class HistoryHotRec(conf: Map[String, String]) {
    val logger = LoggerFactory.getLogger(classOf[HistoryHotRec])
    /**
      * 基于历史热门的推荐, 统计历史评分数据(> 3.0)中电影评分数据最多的电影
      * @param spark sparkSession
      * @param conf conf
      */
    def fit(spark: SparkSession): Unit = {
        val historyHotDF = spark.sql("select mid, count(mid) as count from rating where score >= 3.0 group by mid order by count desc")
        logger.info("History hot rec.")
        historyHotDF.show()
        implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
        StorageUtil.storeResult2Mysql(historyHotDF, Constant.HISTORY_HOT_REC)
    }
}
