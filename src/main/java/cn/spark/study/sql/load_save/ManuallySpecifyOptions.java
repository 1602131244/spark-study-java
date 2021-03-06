package cn.spark.study.sql.load_save;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 手动指定数据源类型
 * @author GYJ
 * 2017-12-04
 */
public class ManuallySpecifyOptions {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				//.master("local")
				.appName("ManuallySpecifyOptions")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		Dataset<Row> peopleDF = spark.read().format("json").load("hdfs://spark1:9000/people.json");
		peopleDF.select("name","age").write().format("parquet").save("hdfs://spark1:9000/people_name_age_java");
	}
}
