# 1. init a client class
# 2. fetches metadata at startup
# 3. actions:
    # a. read data
    # b. write data using spark to s3 in msgpack format

# requires following jars for spark
# /io/github/cybercentrecanada/spark-msgpack-datasource-3.5_2.12/1.9.0/spark-msgpack-datasource-3.5_2.12-1.9.0.jar
# /org/msgpack/msgpack-core/0.9.8/msgpack-core-0.9.8.jar
# /org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
# /com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar