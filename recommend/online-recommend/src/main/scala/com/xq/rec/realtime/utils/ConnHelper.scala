package com.xq.rec.realtime.utils

import redis.clients.jedis.Jedis

object ConnHelper extends Serializable {
    lazy val redis = new Jedis("localhost")
}
