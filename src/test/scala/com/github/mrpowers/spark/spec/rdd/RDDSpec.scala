package com.github.mrpowers.spark.spec.rdd

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.scalatest.FunSpec
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

class RDDSpec extends FunSpec with DataFrameSuiteBase with RDDComparisons {

  describe("#collect") {

    it("return an array that contains all of the elements in this RDD") {

      val xs = (1 to 3).toList
      val rdd = sc.parallelize(xs)
      assert(rdd.collect().deep === Array(1, 2, 3).deep)

    }

  }

  describe("#filter") {

    it("returns a new RDD containing only the elements that satisfy a predicate") {

      val xs = (1 to 5).toList
      val sourceRdd = sc.parallelize(xs)
      val actualRdd = sourceRdd.filter{ n => n >= 3}

      val expectedRdd = sc.parallelize(
        (3 to 5).toList
      )

      assertRDDEquals(actualRdd, expectedRdd)

    }

  }

  describe("flatMap") {

    it("return a new RDD by first applying a function to all elements of this RDD, and then flattening the results") {

      val xs = List("this is something long")
      val sourceRdd = sc.parallelize(xs)
      val actualRdd = sourceRdd.flatMap{ l => l.split(" ") }

      val expectedRdd = sc.parallelize(
        List("this", "is", "something", "long")
      )

      assertRDDEquals(actualRdd, expectedRdd)

    }

  }

  describe("#intersect") {

    it("returns the intersection of this RDD and another one") {

      val xs1 = (1 to 5).toList
      val rdd1 = sc.parallelize(xs1)

      val xs2 = (4 to 8).toList
      val rdd2 = sc.parallelize(xs2)

      assert(xs1.intersect(xs2) === List(4, 5))

    }

  }

  describe("#map") {

    it("returns a new RDD by applying a function to all elements of this RDD") {

      val sourceData = List(
        ("cat"),
        ("dog"),
        ("frog")
      )
      val sourceRdd: RDD[String] = sc.parallelize(sourceData)

      val actualRdd: RDD[Int] = sourceRdd.map{ l => l.length }

      val expectedData = List(
        (3),
        (3),
        (4)
      )
      val expectedRdd: RDD[Int] = sc.parallelize(expectedData)

      assertRDDEquals(actualRdd, expectedRdd)
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
