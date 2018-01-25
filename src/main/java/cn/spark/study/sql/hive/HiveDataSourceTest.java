package cn.spark.study.sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hive数据源
 * @author GYJ
 * 2017-12-11
 *
 */
@SuppressWarnings("deprecation")
public class HiveDataSourceTest {

	public static void main(String[] args) {
//		//创建SaprkSession
//		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
//		SparkSession spark = SparkSession
//				.builder()
//				.appName("HiveDataSource")
//				.config("spark.sql.warehouse.dir", warehouseLocation)
//				.enableHiveSupport()
//				.getOrCreate();
		//创建SparkConf 
		SparkConf conf = new SparkConf();
	    //创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		//创建HiveContext，注意，这里它接收的是SparkContext作为参数，而不是JavaSparkContext
		HiveContext sqlContext = new HiveContext(sc.sc());
		
		
		//第一个功能，使用SparkSession的sql()/hql()方法，可以执行Hive中能够执行的HiveQL 语句
		
		//将学生基本信息数据导入student_infos
		
		//判断是否存在student_info表，如果存在则删除
		sqlContext.sql("DROP TABLE IF EXISTS student_infos");
		
		//判断student_info是否不存在，不存在则创建表
		sqlContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
		
		//将学生基本信息数据导入student_infos表中
		sqlContext.sql("LOAD DATA LOCAL INPATH '/opt/modules/spark-study/resource/student_infos.txt' INTO TABLE student_infos");
		
		//用同样的方式将student_scores导入数据
		sqlContext.sql("DROP TABLE IF EXISTS student_scores");
		sqlContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
		sqlContext.sql("LOAD DATA LOCAL INPATH '/opt/modules/spark-study/resource/student_scores.txt' INTO TABLE student_scores");
		
		
		//第二个功能，执行SQL还可以返回DataFrame，用于查询
		//执行SQL查询，关联两张表，查询成绩大于80成绩的学生
		
		Dataset<Row> goodStudentDF = sqlContext.sql(" SELECT si.name,si.age,ss.score"
				+" FROM student_infos si "
				+" JOIN student_scores ss ON si.name=ss.name "
				+ "WHERE ss.score>=80 ");
		
		
		//第三个功能，可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
		//将DataFrame中的数据存到Hive表中
		
	    //接着将DataFrame中的数据保存到good_student_infos表中
		
		sqlContext.sql("DROP TABLE IF EXISTS good_student_infos");
		goodStudentDF.write().saveAsTable("good_student_infos");
		
		//第四个功能，可以用table()方法，针对hive表，直接创建DataFrame
		//然后针对good_student_infos表，直接创建DataFrame
		
		Row[] goodStudentRows= (Row[]) sqlContext.table("good_student_infos").collect();
		
		for (Row row : goodStudentRows) {
			System.out.println(row);
		}
		
		sc.close();
	}
}
