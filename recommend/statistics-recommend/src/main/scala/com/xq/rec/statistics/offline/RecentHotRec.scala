package com.xq.rec.statistics.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.xq.rec.statistics.constant.Constant
import com.xq.rec.statistics.executor.StatisticsRecommend.logger
import com.xq.rec.statistics.model.MysqlConf
import com.xq.rec.statistics.utils.StorageUtil
import org.apache.spark.sql.SparkSession

/**
  * 近期热门电影推荐
  */
class RecentHotRec(conf: Map[String, String]) {

    /**
      * 将电影评分数据按照yyyyMM日期分组，按照电影评分数据( > 3.0 )统计评分最多的电影
      * @param spark sparkSession
      */
    def fit(spark: SparkSession): Unit = {
        val sdf = new SimpleDateFormat("yyyyMM")
        // 自定义函数udf使用场景：如果在spark.sql()中的SQL语句中使用，则要使用spark.udf.register()来注册，要是在DataFrame中的select()中使用，可以使用udf()函数来注册
        // import org.apache.spark.sql.functions._
        // val formatDate = udf((timestamp: Int) => sdf.format(new Date(timestamp * 1000L)).toInt)
        spark.udf.register("formatDate", (timestamp: Int) => sdf.format(new Date(timestamp * 1000L)).toInt)
        // 自定义函数跟聚合函数一起使用时会报错，需要先将自定义函数得到的列封装成一张表
        val recentTableDF = spark.sql("select mid, formatDate(timestamp) as yearmonth from rating")
        recentTableDF.createOrReplaceTempView("recent")
        recentTableDF.show()
        val recentDF = spark.sql("select mid, yearmonth, count(mid) as count from recent group by yearmonth, mid order by yearmonth desc, count desc")
        logger.info("Recent hot rec...")
        recentDF.show()
        implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
        StorageUtil.storeResult2Mysql(recentDF, Constant.RECENT_HOT_REC)
    }
}
