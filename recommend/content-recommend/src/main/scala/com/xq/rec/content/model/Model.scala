package com.xq.rec.content.model

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
  * 基于电影内容提取特征的相似度推荐列表
  * @param mid mid
  * @param recs 电影推荐列表，mid|mid|mid，该列表是基于相似度计算的电影列表
  */
case class ContentMovieRecs(mid: String, recs: String)

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