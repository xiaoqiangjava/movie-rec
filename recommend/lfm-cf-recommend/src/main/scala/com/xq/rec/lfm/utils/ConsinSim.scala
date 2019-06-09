package com.xq.rec.lfm.utils

import org.jblas.DoubleMatrix

/**
  * 计算连个向量的余弦相似度
  */
object ConsinSim {

    /**
      * 计算两个向量的余弦相似度  A * B / (||A|| * ||B||)
      * @param a
      * @param b
      * @return
      */
    def consinSim(a: DoubleMatrix, b: DoubleMatrix): Double = {
        a.dot(b) / (a.norm2() * b.norm2())
    }
}
