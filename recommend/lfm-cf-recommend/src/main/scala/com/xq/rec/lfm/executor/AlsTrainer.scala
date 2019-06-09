package com.xq.rec.lfm.executor

import java.util.Properties

import breeze.numerics.sqrt
import com.xq.rec.lfm.constant.Constant
import com.xq.rec.lfm.model.MovieRating
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * ALS算法中最优参数选取
  */
object AlsTrainer {
    val logger = LoggerFactory.getLogger(AlsTrainer.getClass)

    val conf = Map("spark.master" -> "local",
        "mysql.url" -> "jdbc:mysql://192.168.0.103:3306/recommend",
        "mysql.driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "password" -> "root")
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(AlsTrainer.getClass.getSimpleName).setMaster(conf("spark.master"))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        spark.sparkContext.setCheckpointDir("D:/checkpoint")
        // 从mysql中加载数据源
        val props = new Properties()
        props.setProperty("user", conf("user"))
        props.setProperty("password", conf("password"))
        props.setProperty("driver", conf("mysql.driver"))
        import spark.implicits._
        val ratingRDD = spark.read
            .jdbc(conf("mysql.url"), Constant.RATING_COLLECTION, props)
            .as[MovieRating]
            .map(rating => Rating(rating.uid, rating.mid, rating.score))
            .rdd.cache()

        // 将数据划分为训练集和测试集
        val Array(trainSet, testSet) = ratingRDD.randomSplit(Array(0.8, 0.2))
        // 参数调优，取不同的rank和maxIter，分别计算RMSE
        val result = adjustAlsParam(trainSet, testSet)
        logger.info("参数：{}", result)

        spark.close()

    }

    /**
      * Als算法参数调优
      * @param trainSet trainSet
      * @param testSet testSet
      * @return
      */
    def adjustAlsParam(trainSet: RDD[Rating], testSet: RDD[Rating]): Array[(Int, Double, Double)] = {
        val result = for (rank <- Array(50, 100, 150); lambda <- Array(0.001, 0.01, 0.1)) yield {
            val model = ALS.train(trainSet, rank, 50, lambda)
            val rmse = getRmse(model, testSet)
            (rank, lambda, rmse)
        }
        logger.info("最优参数： {}", result.minBy(_._3))
        result
    }

    def getRmse(model: MatrixFactorizationModel, testSet: RDD[Rating]): Double = {
        // 计算预测评分
        val userItems = testSet.map(item => (item.user, item.product))
        val predictRating = model.predict(userItems)
        // 以uid和mid作为外键
        val observed = testSet.map(item => ((item.user, item.product), item.rating))
        val predict = predictRating.map(rating => ((rating.user, rating.product), rating.rating))
        val error = observed.join(predict).map{
            case ((uid, mid), (actual, pred)) => {
                val err = actual - pred
                err * err
            }
        }.mean()
        sqrt(error)
    }
}
