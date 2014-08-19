name := "Scala Scan"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.0.1"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,
			    "org.apache.spark" %% "spark-sql" % sparkVersion,
                            "org.apache.spark" %% "spark-hive" % sparkVersion)

resolvers ++= Seq("Apache Repository" at "https://repository.apache.org/content/repositories/releases",
	           "Akka Repository" at "http://repo.akka.io/releases/",
		   "Local Repo" at Path.userHome.asFile.toURI.toURL + "/.m2/repository",
		   Resolver.mavenLocal
)


