# spark-spec

A test suite to document the behavior of the Spark engine.

[![Build Status](https://travis-ci.org/snithish/spark-spec.svg?branch=master)](https://travis-ci.org/snithish/spark-spec)

## Goals

1. Augment the official documentation with working code snippets.

2. Document unexpected behavior in the Spark engine.

3. Make it easier to identify what breaks when Spark versions change.

4. Create a ton of drama, [like the RubySpec project](https://news.ycombinator.com/item?id=8821015).  Just kidding!

## Usage

You can run the entire test suite with the `sbt test`command.

You can run a single tests file with `sbt "test-only *DatasetSpec"`.

## Example

Here's a simple example that documents the behavior of the `Dataset#count` method:

```scala
describe("#count") {

  it("returns a count of all the rows in a DataFrame") {

    val sourceDf = Seq(
      ("jets"),
      ("barcelona")
    ).toDF("team")

    assert(sourceDf.count === 2)

  }

}
```

## Contribution

We are actively looking for contributors to add tests for new methods and augment existing tests to cover edge cases.

To get started, fork the project and submit a pull request.  Methods that aren't documented yet are marked as `pending` and are a good place to start.

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

Continued excellence will be rewarded with push access to the master branch.
