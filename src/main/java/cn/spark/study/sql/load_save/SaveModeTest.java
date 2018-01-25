package cn.spark.study.sql.load_save;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * SaveMode实例
 * @author GYJ
 * 2017-12-4
 *
 */
public class SaveModeTest {

	public static void main(String[] args) {
		SparkSession spark =SparkSession
				.builder()
				.master("local")
				.appName("SaveMode")
				.config("spark.some.config.option","some-value")
				.getOrCreate();
		Dataset<Row> peopleDF = spark.read().format("json").load("E://spark视频//测试数据//people.json");
		peopleDF.write().format("json").mode(SaveMode.Overwrite).save("E://spark视频//测试数据//123");
	}
		

	
}
