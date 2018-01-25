package cn.spark.study.sql.RDD2DataFrame;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import cn.spark.study.sql.entity.Student;

/**
 * 使用反射的方式将RDD转化为DataFrame
 * 
 * @author GYJ 2017-12-1
 */
public class RDD2DataFrameReflection {

	public static void main(String[] args) {

		// 创建SparkConf
		SparkConf conf = new SparkConf()
						 .setMaster("local")
						 .setAppName("RDD2DataFrameReflection");
		//创建JavaSparkContext
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//创建SQLContext
		
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sc);
		
		//创建普通RDD
		
		//JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//student.txt");
		JavaRDD<String> lines = sc.textFile("E://spark视频//测试数据//student.txt");

		JavaRDD<Student> studentRdd = lines.map(new Function<String, Student>() {

			private static final long serialVersionUID = 1L;

			public Student call(String line) throws Exception {
				
				String[] lineSplited = line.split(",");
				Student student = new Student();
				student.setId(Integer.parseInt(lineSplited[0].trim()));;
				student.setName(lineSplited[1]);
				student.setAge(Integer.parseInt(lineSplited[2].trim()));
				return student;
			}
		});
		
		
		//使用反射方式，将RDD转化为DataFrame
		
		//将Student.class传入进去，其实就是用反射的方式来创建DataFrame
		
		//student.class本身就是一个反射的作用
		
		//然后底层还是通过对student.calss进行反射，来获取其中的field
		
		
		Dataset<Row> df = sqlContext.createDataFrame(studentRdd, Student.class);
		
		
		//拿到DataFrame就可以将其注册为一个临时表，然后对其中的数据，执行SQL语句
		
		df.createOrReplaceTempView("students");
		
		//针对students临时表，执行SQL语句、查询年龄<=18的青少年，teenager
		
		Dataset<Row> teenagerDF = sqlContext.sql("select * from students where age<=18");
		
		
		//将查询出来的DataFrame再次转换成RDD
		
		JavaRDD<Row> teenageRDD = teenagerDF.javaRDD();
		
		//将RDD的数据进行映射，映射成student
		
		JavaRDD<Student> teenagerStudentRDD = teenageRDD.map(new Function<Row, Student>() {

			private static final long serialVersionUID = 1L;

			public Student call(Row row) throws Exception {
				Student student = new Student();
				student.setAge(row.getInt(0));
				student.setId(row.getInt(1));
				student.setName(row.getString(2));
				return student;
			}
		});
		
		//将数据collect ，在控制台打印
		
		
		List<Student> studentList = teenagerStudentRDD.collect();
		
	    for (Student student : studentList) {
			System.out.println(student);
		}
		
		
		
		
		
		
		
	

	}

	
}
