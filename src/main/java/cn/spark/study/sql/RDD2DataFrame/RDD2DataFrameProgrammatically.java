package cn.spark.study.sql.RDD2DataFrame;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 
 * 以编程方式动态指定元数据，将RDD转换为DataFrame
 * @author GYJ
 * 2017-12-1
 *
 */
public class RDD2DataFrameProgrammatically {
	
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
								.setMaster("local")
								.setAppName("RDD2DataFrameProgrammatically");
		//创建JavaSaprkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		//创建SQLContext
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sc);
		
		//第一步，创建一个普通的RDD，但是必须将其转换成RDD<Row> 这种格式
		//分析一下
		//报了一个，不能直接从String类型转换成Integer类型的错误
		//就说明，有一个数据，给定义成了String 类型，结果使用的时候，要用Integer类型来使用
		//而且错误报在SQL相关的代码中，基本可以断定，就是说，在sql 中
		//用到age<18的语法，所以就强行将age转换为integer来使用
		//肯定是之前有些步骤，将age定义为String
		//所以往前找，就找到了这里，
		//往row中赛数据的时候，要注意，什么格式的数据，就用什么格式转换一下
		JavaRDD<String> lines = sc.textFile("E://spark视频//测试数据//student.txt");
		JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {

			private static final long serialVersionUID = 1L;

			public Row call(String line) throws Exception {
				String[] lineSplited = line.split(",");
				return RowFactory.create(Integer.valueOf(lineSplited[0].trim()),
										lineSplited[1].trim(),
										Integer.valueOf(lineSplited[2].trim()));
			}
		});
		
		
//		JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
//
//			private static final long serialVersionUID = 1L;
//
//			public Row call(String line) throws Exception {
//				String[] lineSplited = line.split(",");
//				return RowFactory.create(lineSplited[0].trim(),
//										lineSplited[1].trim(),
//										lineSplited[2].trim());
//			}
//		});
		
		//第二步，动态构造元数据
		//比如说，id，bean等，field的名称和类型，可能都是在程序运行过程中，动态从SQL  db里
		//或者是配置文件中，加载出来，是不固定的，
		//所以特别适合用编程的方式来构造元数据
		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		
//		String schemaString = "id name age";
//		List<StructField> structFields = new ArrayList<StructField>();
//		for(String fieldName: schemaString.trim().split(" ")){
//			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
//			structFields.add(field);	
//		}
		
		StructType schema = DataTypes.createStructType(structFields);
		
		
		//第三步：使用动态构造的元数据，将RDD转换为DataFrame
		
		Dataset<Row> studentDF =sqlContext.createDataFrame(studentRDD, schema);
		
		
		//后面，就可以使用Dataset了
		
		studentDF.createOrReplaceTempView("students");
		
		Dataset<Row> results = sqlContext.sql("select * from students where age<=18");
		
		List<Row> teenagerRDD = results.javaRDD().collect();
		
		for (Row row : teenagerRDD) {
			System.out.println(row);
			
		}

	}
}
