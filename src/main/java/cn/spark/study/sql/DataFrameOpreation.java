package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataFrame的常用操作
 * @author GYJ
 * 2017-11-30
 */
public class DataFrameOpreation {
	
	public static void main(String[] args) {
		
		//创建DataFrame
		SparkSession spark = SparkSession
				  .builder()
				  //.master("local")
				  .appName("DataFrameCreate")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		
		//创建出来的DataFrame可以理解为一张表
		Dataset<Row> df = spark.read().json("hdfs://spark1:9000/students.json");
		//Dataset<Row> df = spark.read().json("C://Users//Administrator//Desktop//students.json");
		
		
		//打印DataFrame的所有数据
		df.show();
		
		//打印DataFrame的元数据（Schema）
		
		df.printSchema();
		
		//查询某列所有的数据
		
		df.select("name").show();
		
		//查询某几列所有的数据,并对列进行计算
		
		df.select(df.col("name"),df.col("age").plus(1)).show();
		
		//根据某一列的值进行过滤
		
		df.filter(df.col("age").gt(18)).show();
		
		//根据某一列进行分组，然后进行聚合
		
		df.groupBy(df.col("age")).count().show();
		
		
		
		
		
	}
}
