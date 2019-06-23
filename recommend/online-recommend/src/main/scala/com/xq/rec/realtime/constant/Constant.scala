package com.xq.rec.realtime.constant

import org.apache.kafka.common.serialization.StringDeserializer

object Constant {
    /**
      * 最近评分的电影数量
      */
    val RECENT_RATING_NUM = 20

    /**
      * 相似电影的候选集列表数量
      */
    val SIM_MOVIE_NUM = 20
    /**
      * item rec list
      */
    val MOVIE_RECS = "rec_movie_movies"

    /**
      * movie rating list
      */
    val MOVIE_RATING = "rec_rating"

    /**
      * real-time recommend list
      */
    val REAL_TIME_RECS = "rec_real_time"

    /**
      * kafka config
      */
    val kafkaConf = Map(
        "bootstrap.servers" -> "learn:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "recommend",
        "auto.offset.reset" -> "latest"
    )
}
