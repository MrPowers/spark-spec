package com.github.mrpowers.spark.spec.sql.types

import org.apache.spark.sql.types.MetadataBuilder
import org.scalatest.FunSpec

class MetadataSpec extends FunSpec {

  describe("#contains") {

    it("returns true if the Metadata object contains a key") {

      val m = new MetadataBuilder().putString("comment", "crazy").build()
      assert(m.contains("comment") === true)

    }

    it("should not error out if the Metadata object doesn't contain a key") {

      val m = new MetadataBuilder().putString("comment", "crazy").build()
      assert(m.contains("bulbasaur") === false)

    }

  }

  describe("#equals") {

    it("returns true if another Metadata object is the same") {

      val m1 = new MetadataBuilder().putString("comment", "crazy").build()
      val m2 = new MetadataBuilder().putString("comment", "crazy").build()
      assert(m1.equals(m2) === true)

    }

  }

  describe("#getBoolean") {

    it("gets a boolean metadata value") {

      val m = new MetadataBuilder().putBoolean("isFun", true).build()
      assert(m.getBoolean("isFun"), true)

    }

  }

  describe("#getBooleanArray") {
    pending
  }

  describe("#getDouble") {
    pending
  }

  describe("#getDoubleArray") {
    pending
  }

  describe("#getLong") {
    pending
  }

  describe("#getLongArray") {
    pending
  }

  describe("#getMetadata") {
    pending
  }

  describe("#getMetadataArray") {
    pending
  }

  describe("#getString") {
    pending
  }

  describe("#getStringArray") {
    pending
  }

  describe("#hastCode") {
    pending
  }

  describe("#json") {
    pending
  }

  describe("#toString") {
    pending
  }

}
