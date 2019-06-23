package com.xq.rec.realtime.executor

import java.util.Properties

import com.xq.rec.realtime.constant.Constant
import com.xq.rec.realtime.model.{MovieRecs, Rating}
import com.xq.rec.realtime.recommend.RealTimeRec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * 实时推荐模块
  */
object RealTimeRecommend {
    val logger = LoggerFactory.getLogger(RealTimeRecommend.getClass)
    val conf = Map("spark.master" -> "local[2]",
        "mysql.url" -> "jdbc:mysql://192.168.0.103:3306/recommend",
        "mysql.driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "password" -> "root",
        "kafka.topic" -> "recommend"
    )
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        // 使用local模式时，线程数量必须大于1，否则不会消费数据
        sparkConf.setMaster(conf("spark.master")).setAppName(RealTimeRecommend.getClass.getSimpleName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // 创建StreamingContext，用于实时计算
        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
        ssc.checkpoint("d:/checkpoint")
        // 加载电影相似度矩阵数据，该数据在离线阶段写入到mysql中
        val props = new Properties()
        props.setProperty("user", conf("user"))
        props.setProperty("password", conf("password"))
        import spark.implicits._
        // 从mysql中加载电影相似度列表
        val simMovies = spark.read
            .jdbc(conf("mysql.url"), Constant.MOVIE_RECS, props)
            .as[MovieRecs]
            .map(movieRecs => { //为了查询方便，将电影相似度列表转换成Map[String, Map[String, Double]]
                val recs = movieRecs.recs
                val recsMap = recs.split("\\|")
                    .map(str => (str.substring(1, str.length-1).split(",")(0).toInt, str.substring(1, str.length-1).split(",")(1).toDouble))
                    .toMap
                (movieRecs.mid.toInt, recsMap)
            }).rdd.collectAsMap()
        logger.info("加载电影相似度列表完成，size:{}", simMovies.size)
        // 广播电影相似度map，广播变量不能广播RDD，可以广播RDD的结果，上面的SimMovies就是RDD操作之后的结果
        // 广播变量的目的是为了后面实时计算的时候查询时节省Executor端的内存，不然每个Executor中的每个task都保存一份副本
        val simVoivesBroadcast = spark.sparkContext.broadcast(simMovies)

        // 从mysql中加载用户已经评分过的电影列表，用户推荐时的过滤
        val ratingMovies = spark.read
            .jdbc(conf("mysql.url"), Constant.MOVIE_RATING, props)
            .as[Rating]
            .map(rating => (rating.uid, rating.mid)).collect()
        val ratingMoviesBroadcast = spark.sparkContext.broadcast(ratingMovies)

        // 从kafka中读取实时流数据, 数据格式为uid|mid|score|timestamp
        val kafkaDStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](Array(conf("kafka.topic")), Constant.kafkaConf))
        // 基于实时流推荐
        val realTimeRec = new RealTimeRec()
        realTimeRec.fit(simVoivesBroadcast.value, ratingMoviesBroadcast.value, kafkaDStream)

        ssc.start()
        ssc.awaitTermination()
    }
}
