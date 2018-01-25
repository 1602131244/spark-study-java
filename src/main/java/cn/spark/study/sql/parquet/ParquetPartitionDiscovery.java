package cn.spark.study.sql.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Parquet数据源之自动推断分区
 * @author GYJ
 * 2017-12-7
 */
public class ParquetPartitionDiscovery {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("ParquetPartitionDiscovery")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		Dataset<Row> userDF = spark.read()
				.parquet("hdfs://spark1:9000/spark-study/users/gender=male/country=US/users.parquet");
		userDF.printSchema();
		userDF.show();
		
	}
}
