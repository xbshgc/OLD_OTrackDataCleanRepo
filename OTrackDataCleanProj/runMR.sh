hadoop fs -rmr hdfs://127.0.0.1/user/sample/macdata/output

hadoop jar ./target/dataclean.jar com.shgc.otrack.dataclean.DataCleanDriver hdfs://127.0.0.1/user/sample/macdata/macdata hdfs://127.0.0.1/user/sample/macdata/output

