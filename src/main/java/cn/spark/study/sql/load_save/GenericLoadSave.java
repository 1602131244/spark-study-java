package cn.spark.study.sql.load_save;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * Load and save 的parquet文件操作
 * 
 * @author GYJ
 * 2017-12-4
 */
public class GenericLoadSave {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder()
				  .master("local")
				  .appName("GenericLoadSave")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		Dataset<Row> usersDF =spark.read().load("hdfs://spark1:9000/users.parquet");
		usersDF.printSchema();
		usersDF.show();
		usersDF.select("name","favorite_color").write().save("hdfs://spark1:9000/nameAndColors.parquet");
		
		
	}
}
