# spark-spec

A test suite to document the behavior of the Spark engine.

## Goals

1. Create a test suite that's easier to read than the Spark source code.

2. Make it easier to identify what breaks when Spark versions change.

3. Augment the official documentation with working code snippets.

4. Create a ton of drama, [like the RubySpec project](https://news.ycombinator.com/item?id=8821015).  Just kidding!

## Usage

You can run the entire test suite with the `sbt test`command.

You can run a single tests file with `sbt "test-only *DatasetSpec"`.

## Contribution

We are actively looking for contributors to add tests for new methods and augment existing tests to cover edge cases.

To get started, fork the project and submit a pull request.

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

Continued excellence will be rewarded with push access to the master branch.
