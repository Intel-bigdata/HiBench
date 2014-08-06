name := "HiBench kmean converter"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.1"

libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.9"

libraryDependencies += "org.apache.mahout" % "mahout-math" % "0.9"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Apache Repository" at "https://repository.apache.org/content/repositories/releases"

resolvers += "Spray Repository" at "http://repo.spray.cc/"
