package com.xq.rec.realtime.recommend

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.xq.rec.realtime.constant.Constant
import com.xq.rec.realtime.executor.RealTimeRecommend.conf
import com.xq.rec.realtime.model.MysqlConf
import com.xq.rec.realtime.utils.ConnHelper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 实时推荐算法
  */
class RealTimeRec extends Serializable {
    val logger = LoggerFactory.getLogger(getClass)
    /**
      * 根据电影相似度和电影评分加权获取当前用户推荐优先级
      * @param simMovies 相似度电影列表
      * @param ratingMovies 用户已经评分过的电影列表
      */
    def fit(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
            ratingMovies: Array[(Int, Int)],
            kafkaDStream: DStream[ConsumerRecord[String, String]]): Unit = {
        // 将kafka中的数据转成Rating结构
        val ratingDStream = kafkaDStream.map(splitKafkaStream)
        // 基于实时流推荐
        ratingDStream.foreachRDD(msgRDD => {
            msgRDD.foreach{
                case (uid, mid, score) => {
                    println("consume a batch data >>>>>>>>>>>>")
                    // 1. 从Redis中获取当前用户最近的K次评分，保存成Array(mid, score)
                    val recentRatintMovies = getRecentRatingMovies(Constant.RECENT_RATING_NUM, uid, ConnHelper.redis)
                    logger.info("uid({})最近评分电影：{}", uid, recentRatintMovies.take(20))
                    // 2. 从电影相似度矩阵中取出当前电影相似的N个电影，作为备选列表，Array(mid)
                    val candidateMovies = getSimMovies(Constant.SIM_MOVIE_NUM, uid, mid, simMovies, ratingMovies)
                    logger.info("相似度候选集：{}", candidateMovies.take(20))
                    // 3. 对每个备选电影计算推荐优先级，得到当前用户的实时推荐列表Array(mid, score)
                    val recMovies = recommendMovies(candidateMovies, recentRatintMovies, simMovies)
                    logger.info("优先级推荐候选集：{}", recMovies.take(20))
                    // 4. 将推荐结果保存到mysql中(uid, (mid, score)|(mid, score))
                    implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
                    saveMovies2Mysql(uid, recMovies)
                }
            }
        })
    }

    def splitKafkaStream(msg: ConsumerRecord[String, String]): (Int, Int, Double) = {
        val fields = msg.value().split("\\|")
        (fields(0).trim.toInt, fields(1).trim.toInt, fields(2).trim.toDouble)
    }

    /**
      * 获取当前用户最近K次评分的电影列表
      * Redis中保存的评分数据结构为：
      * uid:1 -> ["1:3.0", "2:4.0", "3:5.0"]
      * @param num num
      * @param uid uid
      * @param redis redis
      * @return movies
      */
    def getRecentRatingMovies(num: Int, uid: Int, redis: Jedis): Array[(Int, Double)] = {
        // 该包可以将Java中的类转成scala中的类
        import scala.collection.JavaConversions._
        // 从Redis中获取用户最近评分的电影数据
        redis.lrange("uid:" + uid, 0, num -1)
            .map(item => {
                val fields = item.split(":")
                (fields(0).trim.toInt, fields(1).trim.toDouble)
            })
            .toArray
    }

    /**
      * 获取跟当前评分电影相似的的候选集列表
      * @param num 相似电影的数量
      * @param uid 用于过滤掉当前用于已经评分过的电影
      * @param mid 当前电影ID
      * @param simMovies 电影相似列表
      * @param ratingMovies 用户已经观看过的电影列表 Array[(uid, mid)]
      * @return movies  过滤之后的备选电影列表
      */
    def getSimMovies(num: Int, uid: Int, mid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
                     ratingMovies: Array[(Int, Int)]): Array[Int] = {
        // 1. 从相似电影列表中拿到与当前电影相似的所有电影列表
        val allSimMovies = simMovies(mid).toArray
        // 2. 从评分电影列表中找出当前用户已经观看过的电影列表
        val seenMovies = ratingMovies.filter(_._1 == uid).map(_._2)
        // 3. 从相似度电影列表中过滤掉用户已经观看过的电影
        allSimMovies.filter(x => !seenMovies.contains(x._1)).sortBy(-_._2).take(num).map(_._1)
    }

    /**
      * 计算推荐电影列表：
      * 自定义算法，将相似电影候选集中的电影与用户最近K次评分的电影计算相似度，乘以用户的评分，得到得分加权的相似度
      * 再加上相应的增强因子和减弱因子，得到推荐电影的优先级
      * sum(sim(q, r) * Rr) / sim_sum + lg max(incount, 1) - lg max(recount, 1)
      * q 是根据相似度计算得到的电影候选集
      * r 是用户最近评分过的电影
      * Rr 表示用户对电影r的评分
      * incount 表示评分大于3.0的电影数量
      * recount 表示评分小于3.0的电影数量
      * @param candidateMovies 给用户推荐的候选集电影列表 Array[mid]
      * @param recentRatingMovies 用户最近评分过的电影列表 Array[(mid, score)]
      * @param simMovies 电影相似度矩阵 Array[(mid, sim)]
      * @return recMovies 电影优先级列表
      */
    def recommendMovies(candidateMovies: Array[Int], recentRatingMovies: Array[(Int, Double)],
                        simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
        val scoreReg = ArrayBuffer[(Int, Double)]()
        val increMap = mutable.HashMap[Int, Int]()
        val decreMap = mutable.HashMap[Int, Int]()
        for (mid <- candidateMovies; rating <- recentRatingMovies) {
            // 获取两个电影的相似度
            val sim = simMovies.get(mid) match {
                case Some(simMovie) => simMovie.get(rating._1) match {
                    case Some(sim) => sim
                    case None => 0.0
                }
                case None => 0.0
            }
            // 计算评分与相似度加权后的得分
            scoreReg.append((mid, sim * rating._2))

            // 统计与当前电影相似的电影中，评分大于3.0的数量
            if (sim > 0.7){
                if (rating._2 > 3.0) {
                    increMap(mid) = increMap.getOrElse(mid, 0) + 1
                }else{
                    decreMap(mid) = decreMap.getOrElse(mid, 0) + 1
                }
            }
        }
        scoreReg.groupBy(_._1).map{
            case (mid, scores) => (mid, scores.map(_._2).sum / scores.length + Math.log10(increMap(mid) - Math.log10(decreMap(mid))))
        }.toArray
    }

    /**
      * 将数据保存到mysql中
      * @param uid uid
      * @param candidate 电影候选集(2, 4.5)|(3, 5.0)
      * @param mysqlConf mysqlConf
      */
    def saveMovies2Mysql(uid: Int, candidate: Array[(Int, Double)])(implicit mysqlConf: MysqlConf): Unit = {
        // 保存的动作需要离线处理，不依靠spark，通过jdbc直接插入数据
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try
        {
            conn = DriverManager.getConnection(mysqlConf.uri, mysqlConf.user, mysqlConf.password)
            val sql = "insert into rec_real_time(uid, movies) values (?, ?)"
            stmt = conn.prepareStatement(sql)
            stmt.setInt(1, uid)
            stmt.setString(2, candidate.mkString("|"))
            stmt.executeUpdate()
        }
        catch
        {
            case e: Exception => logger.error(e.getMessage, e)
        }
        finally
        {
            if (null != stmt) {
                stmt.close()
            }
            if (null != conn) {
                conn.close()
            }
        }
    }
}
