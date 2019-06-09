package com.xq.rec.realtime.model

/**
  * MySQL config
  * @param uri uri
  * @param user user
  * @param password password
  */
case class MysqlConf(uri: String, user: String, password: String)

/**
  * 基于用户行为数据的用户推荐列表
  * @param uid uid
  * @param recs 电影推荐列表, mid|mid|mid
  */
case class UserRecs(genres: String, recs: String)

/**
  * 基于用户行为数据的电影推荐列表
  * @param mid mid
  * @param recs 电影推荐列表，mid|mid|mid，该列表是基于相似度计算的电影列表
  */
case class MovieRecs(mid: String, recs: String)

/**
  * Rating 数据集
  * @param uid 用户ID
  * @param mid 电影ID
  * @param score 评分
  * @param timestamp 时间戳
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
