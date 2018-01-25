package cn.spark.study.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;




/**
 * Json数据源
 * @author GYJ
 * 2017-12-7
 *
 */
public class JSONDataSource {

	public static void main(String[] args) {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
							.setAppName("map")
							.setMaster("local");

		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//创建SQLContext
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sc);
		//针对JSON文件,创建DataFrame
		Dataset<Row> studentScoresDF = sqlContext.read().
				json("hdfs://spark1:9000/spark-study/students.json");
		
		//针对学生成绩信息的DataFrame创建临时表，查询分数大于80分的学生的姓名
		//注册临时表，针对临时表执行SQL语句
		studentScoresDF.createOrReplaceTempView("student_score");
		
		Dataset<Row> goodStudentScoresDF = sqlContext.
				sql("select name,score from student_score where score>=80");
		
		//将DataFrame转换为RDD，执行transformation操作
		//针对包含Json串的JavaRDD,创建DataFrame
		List<String> goodStudentNames = goodStudentScoresDF.javaRDD().map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			public String call(Row row) throws Exception {
				
				return row.getString(0);
			}
		}).collect();
		
		
		//然后针对JAVARDD<String> 创建DataFrame
		
		List<String> studentInfoJSONs = new ArrayList<String>();
		studentInfoJSONs.add("{\"name\":\"Leo\",\"age\":18}");
		studentInfoJSONs.add("{\"name\":\"Marry\",\"age\":17}");
		studentInfoJSONs.add("{\"name\":\"Jack\",\"age\":19}");
		
		//将list转换成JavaRDD
		JavaRDD<String> studentInfoRDD = sc.parallelize(studentInfoJSONs);
		
		Dataset<Row> studentInfosDF = sqlContext.read().json(studentInfoRDD);
		
		
		//针对学生基本信息，注册临时表,然后查询学生分数>80的学生的基本信息
		
		studentInfosDF.createOrReplaceTempView("student_infos");
		
		//拼接SQL
		String sqlString = "select name,age from student_infos where name in (";
		
		for (int i=0;i<goodStudentNames.size();i++) {
			sqlString += "'" + goodStudentNames.get(i) + "'";
			
			if(i<goodStudentNames.size()-1){
				sqlString += ",";
			}
		}
		
		sqlString +=")";
		
		
		//获取大于80分学生的基本信息
		Dataset<Row> goodStudentsInfoDF = sqlContext.sql(sqlString);
		
		
		//然后将两份数据的DataFrame，转换成JavaPairRDD，执行Join Transformation 
		//将DataFrame转化为JavaRDD，再map为JavaPairRDD，然后进行join
		JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD = goodStudentScoresDF
				.javaRDD()
				.mapToPair(new PairFunction<Row, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(Row row)
							throws Exception {

						return new Tuple2<String, Integer>(row.getString(0),
								Integer.valueOf(String.valueOf(row.getLong(1))));
					}
				})
				.join(goodStudentsInfoDF.javaRDD().mapToPair(
						new PairFunction<Row, String, Integer>() {

							private static final long serialVersionUID = 1L;

							public Tuple2<String, Integer> call(Row row)
									throws Exception {

								return new Tuple2<String, Integer>(row
										.getString(0), Integer.valueOf(String
										.valueOf(row.getLong(1)).trim()));
							}
						}));
		//然后将封装在RDD中的好学生的全部信息，转化为JavaRDD<Row> 的格式
		
		JavaRDD<Row> goodStudentRowRDD = goodStudentsRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

			private static final long serialVersionUID = 1L;

			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
					throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
		});
		
		//创建一份元数据，将JavaRDD转换为DataFrame
		
		//注册查询好的属性
		List<StructField> structFields = new ArrayList<StructField>();
		
	    structFields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
	    structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType,true));
	    structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType,true));
	    
	    StructType structType = DataTypes.createStructType(structFields);

		Dataset<Row> goodStudentDF = sqlContext.createDataFrame(goodStudentRowRDD, structType);
		
		//最后将好学生的全部信息保存到一个Json文件中
		//将DataFrame中的数据保存到外部的json文件中去
		goodStudentDF.write().json("hdfs://spark1:9000/spark-study/good-students");;
		
		
		
		
		
	}
}
