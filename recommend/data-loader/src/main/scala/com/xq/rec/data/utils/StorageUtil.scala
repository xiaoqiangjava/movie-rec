package com.xq.rec.data.utils

import java.net.InetAddress
import java.util.Properties

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.xq.rec.data.constant.Constant
import com.xq.rec.data.model.{EsConf, MongoConf, MysqlConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

object StorageUtil {
    /**
      * 将数据保存到MongoDB
      * @param movieDF movieDF
      * @param ratingDF ratingDF
      * @param tagDF tagDF
      * @param mongoConf mongoConf
      */
    def storeData2Mongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConf: MongoConf): Unit = {
        // 新建一个mongodb连接
        val mongoClient = MongoClient(MongoClientURI(mongoConf.uri))
        // 如果MongoDB中已经有相应的数据库，删除
        mongoClient(mongoConf.db)(Constant.MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConf.db)(Constant.TAG_COLLECTION).dropCollection()
        mongoClient(mongoConf.db)(Constant.RATING_COLLECTION).dropCollection()
        // 将数据写入MongoDB
        movieDF.write.option("uri", mongoConf.uri)
            .option("collection", Constant.MOVIE_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        ratingDF.write.option("uri", mongoConf.uri)
            .option("collection", Constant.RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        tagDF.write.option("uri", mongoConf.uri)
            .option("collection", Constant.TAG_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        // 对数据表创建索引
        mongoClient(mongoConf.db)(Constant.MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConf.db)(Constant.RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConf.db)(Constant.RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConf.db)(Constant.TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConf.db)(Constant.TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1 ))

        mongoClient.close()
    }

    /**
      * 保存数据到mysql
      * @param movieDF movieDF
      * @param ratingDF ratingDF
      * @param tagDF tagDF
      * @param mysqlConf mysqlConf
      */
    def storeData2Mysql(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mysqlConf: MysqlConf): Unit = {
        val prop = new Properties
        prop.setProperty("user", mysqlConf.user)
        prop.setProperty("password", mysqlConf.password)

        movieDF.write.mode("overwrite").jdbc(mysqlConf.uri, Constant.MOVIE_COLLECTION, prop)
        ratingDF.write.mode("overwrite").jdbc(mysqlConf.uri, Constant.RATING_COLLECTION, prop)
        tagDF.write.mode("overwrite").jdbc(mysqlConf.uri, Constant.TAG_COLLECTION, prop)
    }

    /**
      * 将带有tag信息的电影信息保存到es
      * @param movieDF movieDF
      */
    def storeData2Es(movieDF: DataFrame, spark: SparkSession)(implicit esConf: EsConf): Unit = {
        // 创建es配置
        val settings: Settings = Settings.builder().put("cluster.name", esConf.clusterName).build()
        // 新建一个es客户端
        val esClient = new PreBuiltTransportClient(settings)

        val REGEX_HOST_PORT = "(.+):(\\d+)".r
        esConf.transportHosts.split(",").foreach{
            case REGEX_HOST_PORT(host: String, port: String) => {
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.trim), port.trim.toInt))
            }
        }

        // 先清理遗留的数据
        if (esClient.admin().indices().exists(new IndicesExistsRequest(esConf.index)).actionGet().isExists)
        {
            esClient.admin().indices().delete(new DeleteIndexRequest(esConf.index))
        }
        esClient.admin().indices().create(new CreateIndexRequest(esConf.index))

        // 将数据写入到es
        movieDF.write
            .option("es.nodes", esConf.httpHosts)
            .option("es.http.timeout", "100m")
            .option("es.mapping.id", "mid")
            .mode("overwrite")
            .format("org.elasticsearch.spark.sql")
            .save(esConf.index + "/" + Constant.ES_MOVIE_INDEX)
    }
}
