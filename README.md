# mongodb-perf-test
a simple perf test for mongodb

## Build Project
```shell
mvn clean install
```

## Run Test
```shell
java -cp importer/target/importer-1.0-SNAPSHOT.jar org.terry.importer.MongoImporter --mongodb.database=ycsb --mongodb.collection=agg_test --mongodb.printResult=true --thread=10 --operation.count=100
```