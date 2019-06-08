package com.xq.rec.statistics.model

/**
  * Movie 数据集
  *
  * @param mid 电影ID
  * @param name 电影名称
  * @param desc 详情描述
  * @param timeLong 时长
  * @param issue 发行时间
  * @param shoot 拍摄时间
  * @param language 语言
  * @param genres 分类
  * @param actors 演员
  * @param directors 导演
  */
case class Movie(mid: Int, name: String, description: String, timeLong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
  * Rating 数据集
  * @param uid 用户ID
  * @param mid 电影ID
  * @param score 评分
  * @param timestamp 时间戳
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
  * MongoDB config
  * @param uri uri
  * @param db db
  */
case class MongoConf(uri: String, db: String)

/**
  * MySQL config
  * @param uri uri
  * @param user user
  * @param password password
  */
case class MysqlConf(uri: String, user: String, password: String)

/**
  * 预测
  * @param mid mid
  * @param score score
  */
case class Predict(mid: Int, score: Double)

/**
  * 电影类别topN推荐对象
  * @param genres 类别
  * @param recs 推荐列表
  */
case class GenresRecommendation(genres: String, recs: String)
