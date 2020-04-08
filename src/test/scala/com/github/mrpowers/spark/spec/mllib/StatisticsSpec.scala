package com.github.mrpowers.spark.spec.mllib

import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec
import org.scalactic.TolerantNumerics

class StatisticsSpec extends FunSpec with SparkSessionTestWrapper {

  describe("#corr") {

    it("returns the correlation between arrays") {
      val epsilon = 1e-4f

      implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

      val seriesX: RDD[Double] = spark.sparkContext.parallelize(Array(1, 2, 3, 3, 5))
      val seriesY: RDD[Double] = spark.sparkContext.parallelize(Array(11, 22, 33, 33, 555))
      val correlationPearson: Double = Statistics.corr(seriesX, seriesY, "pearson")
      val correlationSpearman: Double = Statistics.corr(seriesX, seriesY, "spearman")

      assert(correlationPearson === 0.8500286768773001)
      assert(correlationSpearman === 1.0000000000000002)
    }

  }

}
