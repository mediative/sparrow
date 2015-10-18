# Sparrow

Sparrow is a Scala library for converting Spark Dataframe rows to case classes.

[![Build Status](https://travis-ci.org/ypg-data/sparrow.svg)](https://travis-ci.org/ypg-data/sparrow)
[![Join the chat at https://gitter.im/ypg-data/sparrow](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/ypg-data/sparrow)
[![Latest version](https://api.bintray.com/packages/ypg-data/maven/sparrow/images/download.svg)](https://bintray.com/ypg-data/maven/sparrow/_latestVersion)

## Status

The project is still in an experimental state and the API is subject to change
without concerns about backward compatibility.

## Requirements

This library requires Spark 1.3+.

## Limitations and Known Issues

 - Fields of type `java.sql.Timestamp` is not supported.
 - Custom wrapper fields types is not supported.
 - Conversion of certain other field types are not supported.

See the [CodecLimitationsTest](core/src/test/scala/com.mediative.sparrow/CodecLimitationsTest.scala) for details.

## Getting Started

The best way to get started at this point is to read the [API
docs](https://ypg-data.github.io/sparrow/api) and look at the [examples in the
tests](https://github.com/ypg-data/sparrow/tree/master/core/src/test/scala/com.mediative.sparrow).

To use the libray in an SBT project add the following two project settings:

    resolvers += Resolver.bintrayRepo("ypg-data", "maven")
    libraryDependencies += "com.mediative" %% "sparrow" % "0.1.2"

## Building and Testing

This library is built with SBT, which needs to be installed. To run the tests
and build a JAR run the following commands from the project root:

    $ sbt test
    $ sbt package

To build a package for Scala 2.11 run the following command:

    $ sbt ++2.11.7 test package

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute.

## Releasing

To release version `x.y.z` run:

    $ sbt release -Dversion=x.y.z
    $ sbt ++2.11.7 publish

This will take care of running tests, tagging and publishing JARs and API docs
for both version 2.10 and 2.11.

    $ sbt core/spPublish
    $ sbt ++2.11.7 core/spPublish

The above requires that `~/.credentials/spark-packages.properties` exists with
the following content:

    realm=Spark Packages
    host=spark-packages.org
    user=$GITHUB_USERNAME
    # Generate token at https://github.com/settings/tokens
    password=$GITHUB_PERSONAL_ACCESS_TOKEN

## License

Copyright 2015 Mediative

Licensed under the Apache License, Version 2.0. See LICENSE file for terms and
conditions for use, reproduction, and distribution.
