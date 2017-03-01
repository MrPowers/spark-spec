package com.github.mrpowers.spark.spec.rdd

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.scalatest.FunSpec

class RDDSpec extends FunSpec with DataFrameSuiteBase with RDDComparisons {

  describe("#collect") {

    it("return an array that contains all of the elements in this RDD") {

      val xs = (1 to 3).toList
      val rdd = sc.parallelize(xs)
      assert(rdd.collect().deep === Array(1, 2, 3).deep)

    }

  }

  describe("#intersection") {

    it("returns the intersection of this RDD and another one") {

      val xs1 = (1 to 5).toList
      val rdd1 = sc.parallelize(xs1)

      val xs2 = (4 to 8).toList
      val rdd2 = sc.parallelize(xs2)

      assert(xs1.intersect(xs2) === List(4, 5))

    }

  }

  describe("#max") {

    it("returns the max of this RDD as defined by the implicit Ordering[T]") {

      val xs = (1 to 100).toList
      val rdd = sc.parallelize(xs)
      assert(rdd.max() === 100)

    }

  }

}
