name := "Java Bayes bench"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.0.1",
		            "org.apache.spark" %% "spark-mllib" % "1.0.1")

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

