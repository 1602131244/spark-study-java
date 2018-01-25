package cn.spark.study.sql.hive;

import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * Hive数据源
 * @author GYJ
 * 2017-12-22
 *
 */
public class HiveDataResource {

	public static void main(String[] args) {
		//创建SaprkSession
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkSession spark = SparkSession
		  .builder()
		  .appName("Java Spark Hive Example")
		  .config("spark.sql.warehouse.dir", warehouseLocation)
		  .enableHiveSupport()
		  .getOrCreate();
		//第一个功能，使用SparkSession的sql()/hql()方法，可以执行Hive中能够执行的HiveQL 语句
		
		//将学生基本信息数据导入student_infos
		
		//判断是否存在student_info表，如果存在则删除
		spark.sql("DROP TABLE IF EXISTS student_infos");
		//判断student_info是否不存在，不存在则创建表
		spark.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
		spark.sql("LOAD DATA LOCAL INPATH '/opt/modules/spark-study/resource/student_infos.txt' INTO TABLE student_infos");
		
		
		//用同样的方式将student_scores导入数据
		spark.sql("DROP TABLE IF EXISTS student_scores");
		spark.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
		spark.sql("LOAD DATA LOCAL INPATH '/opt/modules/spark-study/resource/student_scores.txt' INTO TABLE student_scores");
		
		
		//第二个功能，执行SQL还可以返回DataFrame，用于查询
	    //执行SQL查询，关联两张表，查询成绩大于80成绩的学生
		
		Dataset<Row> goodStudentDF = spark.sql(" SELECT si.name,si.age,ss.score"
				+" FROM student_infos si "
				+" JOIN student_scores ss ON si.name=ss.name "
				+ "WHERE ss.score>=80 ");
		
		
		//第三个功能，可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
		//将DataFrame中的数据存到Hive表中
		
		
		//第四个功能，可以用table()方法，针对hive表，直接创建DataFrame
	    //接着将DataFrame中的数据保存到good_student_infos表中
		
		spark.sql("DROP TABLE IF EXISTS good_student_infos");
		goodStudentDF.write().saveAsTable("good_student_infos");
		
		Dataset<Row> records = spark.table("good_student_infos");
		
		records.show();
		
		
		
		
	}
}
