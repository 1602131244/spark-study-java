package cn.spark.study.sql.parquet;

import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Parquet数据源之使用编程方式加载数据
 * @author GYJ
 * 2017-12-5
 */
public class ParquetLoadData {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("ParquetLoadData")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		//读取Parquet的文件数据，创建一个DataFrame
		Dataset<Row> userDF = spark.read().parquet("hdfs://spark1:9000/spark-study/users.parquet");
		
		//将DataFrame注册为临时表，然后使用SQL查询需要的数据
		userDF.createOrReplaceTempView("users");
		
		Dataset<Row> userNameDF = spark.sql("select name from users");
		
		//对查询的DataFrame数据，进行一些transformation操作，打印出来
		
		List<String>  userNames = userNameDF.javaRDD().map(new Function<Row, String>() {

			
			private static final long serialVersionUID = 1L;

			public String call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return "Name: "+row.getString(0);
			}
		}).collect();
		
		//打印
		
		for (String userName : userNames) {
			System.out.println(userName);
		}
		
		

		
		
	}
}
