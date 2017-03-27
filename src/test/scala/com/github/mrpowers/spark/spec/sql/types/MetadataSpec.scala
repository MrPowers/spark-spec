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
      assert(m.getBoolean("isFun") === true)

    }

  }

  describe("#getBooleanArray") {
    pending
  }

  describe("#getDouble") {

    it("gets a double metadata value") {

      val m = new MetadataBuilder().putDouble("coolNum", 3.14).build()
      assert(m.getDouble("coolNum") === 3.14)

    }

  }

  describe("#getDoubleArray") {

    it("gets a double array") {

      val m = new MetadataBuilder().putDoubleArray("coolNums", Array(3.14, 8.0)).build()
      assert(m.getDoubleArray("coolNums") === Array(3.14, 8.0))

    }

  }

  describe("#getLong") {

    it("gets a long metadata value") {

      val m = new MetadataBuilder().putLong("bigNumber", 8989L).build()
      assert(m.getLong("bigNumber") === 8989L)

    }

  }

  describe("#getLongArray") {
    pending
  }

  describe("#getMetadata") {

    it("gets a metadata value") {

      val m = new MetadataBuilder().putLong("bigNumber", 8989L).build()
      val m2 = new MetadataBuilder().putMetadata("nestedMeta", m).build()
      assert(m2.getMetadata("nestedMeta") === m)

    }

  }

  describe("#getMetadataArray") {
    pending
  }

  describe("#getString") {

    it("gets a string value") {

      val m = new MetadataBuilder().putString("mood", "sleepy").build()
      assert(m.getString("mood") === "sleepy")

    }

  }

  describe("#getStringArray") {
    pending
  }

  describe("#hashCode") {
    pending
  }

  describe("#json") {

    it("converts a metadata object to JSON") {

      val m = new MetadataBuilder().putString("mood", "sleepy").build()
      assert(m.json === """{"mood":"sleepy"}""")

    }

  }

  describe("#toString") {

    it("converts a metadata object to a string") {

      val m = new MetadataBuilder().putString("mood", "sleepy").build()
      assert(m.toString() === """{"mood":"sleepy"}""")

    }

  }

}
