# EssbaseBatchMigration
Platform agnostic POC to migrate Essbase Applications across different servers in batch.
Official API GUIDE (https://docs.oracle.com/cd/E57185_01/ESBJD/index.html)

- This solutions supports Migration of Essbase application across cross platform servers including all the artifacts along with the data.

Compile:
javac -classpath .:ess_japi.jar Migration.java (Linux)
javac -classpath .;ess_japi.jar Migration.java  (Windows)

Execute:
java -classpath .:ess_japi.jar Migration (Linux)
java -classpath .;ess_japi.jar Migration  (Windows)
