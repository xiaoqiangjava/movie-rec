package com.xq.rec.data.executor

import com.xq.rec.data.constant.Constant
import com.xq.rec.data.model.MysqlConf
import com.xq.rec.data.utils.StorageUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * 加载CSV格式的数据，将数据写入到MongoDB
  */
object DataLoader {
    val logger = LoggerFactory.getLogger(DataLoader.getClass)
    def main(args: Array[String]): Unit = {
        // 定义一个配置项，保存数据加载中使用到的配置信息
        val conf = Map("spark.master" -> "local",
                       "mongo.uri" -> "mongodb://localhost:27017/recommend",
                       "mongo.db" -> "recommend",
                       "es.http.hosts" -> "localhost:9200",
                       "es.transport.hosts" -> "localhost:9300",
                       "es.index" -> "recommend",
                       "es.cluster.name" -> "elasticsearch",
                       "mysql.url" -> "jdbc:mysql://localhost:3306/recommend",
                       "user" -> "root",
                       "password" -> "root")
        // 创建sparkSession
        val spark = SparkSession.builder().appName("DataLoader").master(conf("spark.master")).getOrCreate()

        // 加载电影数据
        logger.info("Start load movie data...")
        val movieSchema = StructType(Constant.movieField)
        val movieOptions = Map("sep" -> "^", "header" -> "false")
        val movieDF = spark.read.schema(movieSchema).options(movieOptions).csv(Constant.MOVIE_DATA_PATH)
        logger.info("End load movie data.")
        movieDF.show()

        // 加载评分数据
        logger.info("Start load rating data...")
        val ratingSchema = StructType(Constant.ratingField)
        val ratingOptions = Map("sep" -> ",", "header" -> "false")
        val ratingDF = spark.read.schema(ratingSchema).options(ratingOptions).csv(Constant.RATING_DATA_PATH)
        logger.info("End load rating data.")
        ratingDF.show()

        // 加载标签数据
        logger.info("Start load tag data...")
        val tagSchema = StructType(Constant.tagField)
        val tagOptions = Map("sep" -> ",", "header" -> "false")
        val tagDF = spark.read.schema(tagSchema).options(tagOptions).csv(Constant.TAG_DATA_PATH)
        logger.info("End load tag data.")
        tagDF.show()

        // 将数据保存到MongoDB
//        implicit val mongoConf = MongoConf(conf("mongo.uri"), conf("mongo.db"))
//        storeData2Mongo(movieDF, ratingDF, tagDF)

        // 将数据保存到mysql
        implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
        StorageUtil.storeData2Mysql(movieDF, ratingDF, tagDF)
        logger.info("Succeed to store data to mysql.")

        // 数据预处理，将movie对应的tag信息添加进去，添加一列tag1|tag2|tag3
        // collect_set函数的作用是将groupBy之后的多列合并成一列
        import org.apache.spark.sql.functions._
        val newTag = tagDF.groupBy(col("mid"))
            .agg(concat_ws("|", collect_set(col("tag"))).as("tags"))
            .select(col("mid"), col("tags"))
        // 将newTag和movie做join
        val movieTag = movieDF.join(newTag, Seq("mid"), "left")
        logger.info("movie join tag...")
        movieTag.show()

        // 将数据保存到elasticsearch
//        implicit val esConf = EsConf(conf("es.http.hosts"), conf("es.transport.hosts"), conf("es.index"), conf("es.cluster.name"))
//        StorageUtil.storeData2Es(movieTag, spark)
        spark.close()
    }
}
