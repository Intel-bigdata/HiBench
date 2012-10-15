MAHOUT_DEP:=$(shell . ../../configure.sh ; echo $$MAHOUT_HOME/examples/target/dependency)

HADOOP_JAR:=$(shell ls $(MAHOUT_DEP)/hadoop-core*.jar | grep -v test)
MAHOUT_CORE_JAR:=$(shell ls $(MAHOUT_DEP)/mahout-core*.jar | grep -v test)
MAHOUT_MATH_JAR:=$(shell ls $(MAHOUT_DEP)/mahout-math*.jar | grep -v test)
UNCOMM_MATH_JAR:=$(shell ls $(MAHOUT_DEP)/uncommons-maths*.jar | grep -v test)
SLF4J_JAR:=$(shell ls $(MAHOUT_DEP)/slf4j-api*.jar)

CLASSPATH:=$(HADOOP_JAR):$(MAHOUT_CORE_JAR):$(MAHOUT_MATH_JAR):$(UNCOMM_MATH_JAR):$(SLF4J_JAR)

all: datagen.jar

datagen.jar: GenKMeansDataset.java
	mkdir -p datagen
	javac -cp $(CLASSPATH) -d datagen GenKMeansDataset.java
	jar -cvf datagen.jar -C datagen .

clean:
	rm -rf datagen

purge: clean
	rm -f datagen.jar
