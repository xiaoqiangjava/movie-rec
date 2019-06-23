package com.xq.rec.content.executor

import java.util.Properties

import com.xq.rec.content.constant.Constant
import com.xq.rec.content.model.Movie
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
import org.slf4j.LoggerFactory

/**
  * 基于内容的推荐，使用TF-IDF提取电影类别关键字
  */
object ContentRecommend {
    val logger = LoggerFactory.getLogger(getClass)

    val conf = Map("spark.master" -> "local",
        "mysql.url" -> "jdbc:mysql://192.168.0.103:3306/recommend",
        "mysql.driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "password" -> "root")

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster(conf("spark.master"))
            .setAppName(ContentRecommend.getClass.getSimpleName)

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 从mysql中加载movies数据，提取类别信息
        val props = new Properties()
        props.setProperty("user", conf("user"))
        props.setProperty("password", conf("password"))
        import spark.implicits._
        val movieTagsDF = spark.read
            .jdbc(conf("mysql.url"), Constant.MOVIE_COLLECTION, props)
            .as[Movie]
            // TF-IDF分词器默认按照空格分词，因此将genres信息中的|字符转成空格
            .map(movie => (movie.mid, movie.name, movie.genres.map(ch => if (ch == '|') ' ' else ch)))
            .toDF("mid", "name", "genres")
            .cache()
        movieTagsDF.show(false)

        // 使用TF-IDF从内容信息中提取电影特征向量
        // 1. 创建一个分词器
        val tokenizer = new Tokenizer()
        tokenizer.setInputCol("genres")
        tokenizer.setOutputCol("words")
        val wordsDF = tokenizer.transform(movieTagsDF)
        wordsDF.show(false)
        // 2. 引入HashingTF，可以把一个词语序列转成对应的词频
        val hashingTF = new HashingTF()
        hashingTF.setInputCol("words")
        hashingTF.setOutputCol("rawFeatures")
        hashingTF.setNumFeatures(50)
        // 得到的结果是稀疏向量数据结构SparseVector(50,[11,13,19],[1.0,1.0,1.0])，第一元素是向量的维度，
        // 第二个元素是有值得下标，第三个是有值元素对应的具体指
        val featureDF = hashingTF.transform(wordsDF)
        featureDF.show(truncate = false)

        // 3. 引入IDF工具，可以通过训练得到IDF模型
        val idf = new IDF()
        idf.setInputCol("rawFeatures")
        idf.setOutputCol("features")
        // 训练idf模型，得到每个词的逆文档评率
        val idfModel = idf.fit(featureDF)
        // 使用模型对原数据进行处理，得到文档中每个词的TF-IDF, 作为新的特征向量
        val rescaleDF = idfModel.transform(featureDF)
        rescaleDF.show(truncate = false)
        // 提取特征信息
        val movieFeatures = rescaleDF.map{
            // SparseVector是spark.ml包中定义的稀疏向量数据结构，可以通过toArray方法转换成正常向量结构，没有的值用0.0补齐
            row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
        }.rdd.map(vector => (vector._1, new DoubleMatrix(vector._2)))

        movieFeatures.collect().foreach(println)

        // 两两计算提取特征之间的相似度，得到相似度推荐列表
        val movieRecs = movieFeatures.cartesian(movieFeatures).cache()
            .filter{
                case (x, y) => x._1 != y._1
            }
            .map{
                case (x, y) => {
                    val sim = cosineSim(x._2, y._2)
                    (x._1, (y._1, sim))
                }
            }
            .filter(_._2._2 > 0.8)
            .groupByKey()
            .map{
                case (mid, recs) => (mid, recs.toList.sortBy(-_._2).mkString("|"))
            }.toDF("mid", "recs")

        movieRecs.show()
        // 将结果保存到mysql中
        movieRecs.write.mode("overwrite")
            .jdbc(conf("mysql.url"), Constant.CONTENT_MOVIE_TABLE, props)

        spark.close()
    }

    def cosineSim(x: DoubleMatrix, y: DoubleMatrix): Double = {
        x.dot(y) / (x.norm2() * y.norm2())
    }
}
