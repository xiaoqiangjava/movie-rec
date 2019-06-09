package com.xq.rec.lfm.executor

import java.util.Properties

import com.xq.rec.lfm.constant.Constant
import com.xq.rec.lfm.model.{MovieRating, MysqlConf}
import com.xq.rec.lfm.utils.{ConsinSim, StorageUtil}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
import org.slf4j.LoggerFactory

/**
  * 基于隐语义模型（LFM）的协同过滤推荐
  */
object LfmCfRecommend {
    val logger = LoggerFactory.getLogger(LfmCfRecommend.getClass)

    val conf = Map("spark.master" -> "local",
        "mysql.url" -> "jdbc:mysql://192.168.0.103:3306/recommend",
        "mysql.driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "password" -> "root")

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        sparkConf.setMaster(conf("spark.master")).setAppName(LfmCfRecommend.getClass.getSimpleName)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        spark.sparkContext.setCheckpointDir("D:/checkpoint")

        // 加载评分数据
        val props = new Properties()
        props.setProperty("user", conf("user"))
        props.setProperty("password", conf("password"))
        // 在本地运行可以不指定driver，但在集群上面运行时需要指定driver
        props.setProperty("driver", conf("mysql.driver"))
        import spark.implicits._
        val ratingRDD = spark.read.
            jdbc(conf("mysql.url"), Constant.RATING_COLLECTION, props).as[MovieRating].rdd
            .map(rating => Rating(rating.uid, rating.mid, rating.score))
            .cache()

        // 定义als算法需要的参数
        val (rank, lambda, maxIter) = (50, 0.01, 10)
        // 训练隐语义模型
        val model = ALS.train(ratingRDD, rank, maxIter, lambda)
        // 1. 为用户推荐电影, 使用ALS内置的方法推荐
        val userMovies = model.recommendProductsForUsers(Constant.USER_MAX_REC)
            .map(recs => (recs._1, recs._2.toList.map(rating => (rating.product, rating.rating)).mkString("|")))
            .toDF("uid", "recs")
        userMovies.show()
        logger.info("使用ALS内置方法推荐的结果：{}", userMovies.take(Constant.USER_MAX_REC))
        implicit val mysqlConf = MysqlConf(conf("mysql.url"), conf("user"), conf("password"))
        StorageUtil.storeResult2Mysql(userMovies, Constant.USER_RECS)
//        // 2. 为用户推荐电影, 自定义方法, 获取用户和物品的笛卡尔积, 使用模型预测评分, 得到评分高的电影列表
//        val userRDD = ratingRDD.filter(_.rating > 3.0).map(rating => rating.user)
//        val movieRDD = ratingRDD.filter(_.rating > 3.0).map(rating => rating.product)
//        // 用户和物品做笛卡尔积，得到一个空的评分矩阵，调用模型的predict方法得到预测评分
//        val predictUserMovie = userRDD.cartesian(movieRDD)
//        predictUserMovie.cache()
//        logger.info("Start predict rating...")
//        val predictRating = model.predict(predictUserMovie)
//        predictRating.cache()
//
//        logger.info("预测评分：{}", predictRating.take(20))
//        val userRecs = predictRating.filter(rating => rating.rating > 0)
//            .map(rating => (rating.user, (rating.product, rating.rating)))
//            .groupByKey()
//            .map(recs => (recs._1, recs._2.toList.sortWith(_._2 > _._2).take(Constant.USER_MAX_REC).mkString("|")))
//            .toDF()
//        logger.info("使用自定方法得到的推荐结果：{}", userRecs.take(Constant.USER_MAX_REC))
//        userRecs.show()

        // 2. 基于电影隐特征计算电影相似度矩阵，得到电影相似度列表
        val movieFeature = model.productFeatures.map({
            case (mid, features) => (mid, new DoubleMatrix(features))
        })
        // 对所有的mid两两计算余弦相似度
        val movieRecs = movieFeature.cartesian(movieFeature)
            .filter({
                case (a, b) => a._1 != b._1
            }).map({
                case (a, b) => {
                    val consinSim = ConsinSim.consinSim(a._2, b._2)
                    (a._1, (b._1, consinSim))
                }
            })
            .filter(_._2._2 > 0.6)
            .groupByKey()
            .map({
                case (mid, recs) => (mid, recs.toList.sortBy(-_._2).mkString("|"))
            })
            .toDF("mid", "recs")
        logger.info("电影相似列表：{}", movieRecs.take(20))
        movieRecs.show()

        StorageUtil.storeResult2Mysql(movieRecs, Constant.MOVIE_RECS)
        spark.close()
    }
}
