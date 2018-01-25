package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 
 * 使用json文件创建DataFrame
 * @author GYJ
 * 2017-11-29
 *
 */
public class DataFrameCreate {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				  .builder()
				  .appName("DataFrameCreate")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		Dataset<Row> df = spark.read().json("hdfs://spark1:9000/students.json");
		//Dataset<Row> df = spark.read().json("C://Users//Administrator//Desktop//students.json");
		// Displays the content of the DataFrame to stdout
		df.show();
		
	
	}
}
