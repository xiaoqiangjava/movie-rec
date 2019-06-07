package com.xq.rec.data.constant

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}

object Constant {
    /**
      * movie table field
      */
    val movieField = List(StructField("mid", IntegerType, false),
        StructField("name", StringType, false),
        StructField("description", StringType, true),
        StructField("timeLong", StringType, false),
        StructField("issue", StringType, false),
        StructField("shoot", StringType, false),
        StructField("language", StringType, false),
        StructField("genres", StringType, true),
        StructField("actors", StringType, true),
        StructField("directors", StringType, true)
    )

    /**
      * rating table field
      */
    val ratingField = List(StructField("uid", IntegerType, false),
        StructField("mid", IntegerType, false),
        StructField("score", DoubleType, false),
        StructField("timestamp", IntegerType, false)
    )

    /**
      * tag table field
      */
    val tagField = List(StructField("uid", IntegerType, false),
        StructField("mid", IntegerType, false),
        StructField("tag", StringType, false),
        StructField("timestamp", IntegerType, false)
    )

    /**
      * movie data path
      */
    val MOVIE_DATA_PATH = "D:\\Learn\\Workspace\\Spark\\movies-res\\recommend\\data-loader\\src\\main\\resources\\movies.csv"

    /**
      * rating data path
      */
    val RATING_DATA_PATH = "D:\\Learn\\Workspace\\Spark\\movies-res\\recommend\\data-loader\\src\\main\\resources\\ratings.csv"

    /**
      * tag data path
      */
    val TAG_DATA_PATH = "D:\\Learn\\Workspace\\Spark\\movies-res\\recommend\\data-loader\\src\\main\\resources\\tags.csv"

    /**
      * mongodb collection -- movie
      */
    val MOVIE_COLLECTION = "rec_movie"

    /**
      * mongodb collection -- rating
      */
    val RATING_COLLECTION = "rec_rating"

    /**
      * mongodb collection -- tag
      */
    val TAG_COLLECTION = "rec_tag"

    /**
      * es movie index
      */
    val ES_MOVIE_INDEX = "recommend"

}
