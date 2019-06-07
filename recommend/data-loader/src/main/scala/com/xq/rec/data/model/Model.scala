package com.xq.rec.data.model

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
  * Tag 数据集
  * @param uid 用户ID
  * @param mid 电影ID
  * @param tag 电影标签
  * @param timestamp 时间戳
  */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

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
  * Elasticsearch config
  * @param httpHosts http主机列表，逗号分隔
  * @param transportHosts transport主机列表
  * @param index 需要操作的索引
  * @param clusterName 集群名称，默认elasticsearch
  */
case class EsConf(httpHosts: String, transportHosts: String, index: String, clusterName: String)
