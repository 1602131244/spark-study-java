package cn.spark.study.sql.hive;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class HiveTest {
	
	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("HiveTest");
//	    JavaSparkContext sc = new JavaSparkContext(conf);
//	    HiveContext hiveContext = new HiveContext(sc.sc());
		
		//创建SaprkSession
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkSession spark = SparkSession
		  .builder()
		  .appName("Java Spark Hive Example")
		  .config("spark.sql.warehouse.dir", warehouseLocation)
		  .enableHiveSupport()
		  .getOrCreate();
		
		
	    //向数据库中写数据
	    //在hive中创建相应的表
	    spark.sql("DROP TABLE IF EXISTS student_infos");
	    spark.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
	    //向tstudent_infos表中加载数据
	    spark.sql("LOAD DATA LOCAL INPATH '/opt/modules/spark-study/resource/student_infos.txt' INTO TABLE student_infos");
	    //创建第二张表
	    spark.sql("DROP TABLE IF EXISTS student_scores");
	    spark.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
	    spark.sql("LOAD DATA LOCAL INPATH '//opt/modules/spark-study/resource/student_scores.txt' INTO TABLE student_scores");
	    //执行多表关联
	    Dataset<Row> joinDF = spark.sql("select b.name, b.age,c.score from student_infos b left join student_scores c on b.name = c.name where c.score >= 80");
	    spark.sql("DROP TABLE IF EXISTS good_student_infos");
	    joinDF.show();
	    joinDF.write().saveAsTable("good_student_infos");
	  
	}
}
