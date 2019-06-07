package com.xq.rec.statistics.constant

object Constant {
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

    /**
      * history hot
      */
    val HISTORY_HOT_REC = "rec_history_hot"

    /**
      * recently hot
      */
    val RECENT_HOT_REC = "rec_recent_hot"

    /**
      * average score
      */
    val AVERAGE_SCORE_REC = "rec_average_score"

    /**
      * genres topN
      */
    val GENRES_TOP_REC = "rec_GENRES_TOP"
}
