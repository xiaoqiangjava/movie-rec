package com.xq.rec.statistics.utils

import java.util.Properties

import com.xq.rec.statistics.model.MysqlConf
import org.apache.spark.sql.DataFrame

object StorageUtil {

    /**
      * 将离线推荐结果保存到mysql
      * @param df df
      * @param table table name
      * @param mysqlConf conf
      */
    def storeResult2Mysql(df: DataFrame, table: String)(implicit mysqlConf: MysqlConf): Unit = {
        val props = new Properties()
        props.setProperty("user", mysqlConf.user)
        props.setProperty("password", mysqlConf.password)
        df.write.mode("overwrite").jdbc(mysqlConf.uri, table, props)
    }
}
