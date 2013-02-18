#! /usr/bin/env sh
cd myudfs
javac -cp ../pig.jar ParseDayFromFileName.java
javac -cp ../pig.jar MyUDF.java
javac -cp ../pig.jar BucketizeByHashCode.java
javac -cp ../pig.jar ExtractTLD.java
javac -cp ../pig.jar FindBucket.java
javac -cp ../pig.jar Dedup.java
cd ../
jar -cf myudfs.jar myudfs
