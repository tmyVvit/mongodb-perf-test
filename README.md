# mongodb-perf-test
a simple perf test for mongodb

## Build Project
build the whole project
```shell
mvn clean install
```

build a single module
```shell
mvn clean install -pl importer
```

## Run Test
```shell
java -cp importer/target/importer-1.0-SNAPSHOT.jar org.terry.importer.MongoImporter --mongodb.database=ycsb --mongodb.collection=agg_test --mongodb.printResult=true --thread=10 --operation.count=100
```