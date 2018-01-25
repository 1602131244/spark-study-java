package cn.spark.study.sql.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * JDBC数据源
 * @author GYJ
 * 2017-12-26
 *
 */
public class JDBCDataSource {
	
	public static void main(String[] args) {
		//创建SaprkSession
		SparkSession spark = SparkSession
				  .builder()
				  .appName("JDBCDataSource")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		//总结一下：
		//jdbc数据源：
		//首先是通过:SparkSession的read系列方法， 加载MySQL中的数据加载为DataFrame；
		//然后可以将DataFrame转化为RDD，使用Spark core 提供的各种算子进行操作
		//最后可以将得到的数据结果，通过foreach()算子，写入mysql,hbase,redis等等db/cache中
		
		
		//分别将mysql中两张表的数据加载为DataFrame
		Map<String, String>  options = new HashMap<String, String>();
		options.put("url", "jdbc:mysql://spark1:3306/testdb");
		options.put("dbtable", "student_infos");
		Dataset<Row> studentInfosDF = spark.read().format("jdbc")
				.options(options).load();
		//
		options.put("dbtable", "student_scores");
		Dataset<Row> studentScoresDF = spark.read().format("jdbc")
				.options(options).load();
		
		//将两个DataFrame转换为JavaPairRDD ,执行join操作
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentRDD=studentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Row row) throws Exception {
				
					return new Tuple2<String, Integer>(row.getString(0),
							Integer.valueOf(String.valueOf(row.get(1))
									));
			}
		}).join(studentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Row row) throws Exception {
			
					return new Tuple2<String, Integer>(row
							.getString(0), Integer.valueOf(String
							.valueOf(row.get(1))));
			}
		}));
		 
		
		//将JavaPairRDD转换为JavaRDD<Row>
		
		JavaRDD<Row> studentRowsRDD = goodStudentRDD.map(
				new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

			private static final long serialVersionUID = 1L;

			public Row call(Tuple2<String, Tuple2<Integer, Integer>> t)
					throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(t._1,t._2._1,t._2._2);
			}
		});
		//过滤出分数大于80分的数据
		JavaRDD<Row> filteredRDD = studentRowsRDD.filter(new Function<Row, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(Row row) throws Exception {
				
				if(row.getInt(2)>80){
					return true;
				}else{
					return false;
				}
			}
		});
		//将JavaRDD转换为DataFrame		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);
		
		Dataset<Row> studentDF = spark.createDataFrame(filteredRDD, structType);
		
		
		studentDF.show();
		
		
		//将DataFrame中的数据保存到mysql表中
//		options.put("dbtable", "good_student_infos");
//		studentDF.write().format("jdbc").options(options).save();
		
		
		
		//在企业中很常用，有可能插入到MySQL，有可能插入到HBASE，还有可能插入Redis缓存
		studentDF.javaRDD().foreach(new VoidFunction<Row>() {
			
			private static final long serialVersionUID = 1L;

			public void call(Row row) throws Exception {
				String sqlString = "insert into good_student_infos values ("
							+ "'"+String.valueOf(row.getString(0))+"',"
							+ Integer.valueOf(String.valueOf(row.get(1)).trim()) + ","
							+ Integer.valueOf(String.valueOf(row.get(2)).trim()) +")";
				Class.forName("com.mysql.jdbc.Driver");
				Connection conn = null;
				Statement stmt = null;
			    try {
					conn = DriverManager.getConnection("jdbc:mysql://spark1:3306/testdb","","");
					stmt = conn.createStatement();
					stmt.executeUpdate(sqlString);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					if (stmt != null) {
						stmt.close();
					}
					if (conn != null) {
						conn.close();
					}
				}
				
				
			}
		});
		


		
		
	}
}
