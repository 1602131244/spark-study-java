package cn.spark.study.sql.built_in_function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
/**
 * 开窗函数以及top3销售额统计案例
 * @author GYJ
 * 2018-1-1
 *
 */
@SuppressWarnings("deprecation")
public class RowNumberWindowFunction {

	public static void main(String[] args) {
//		//创建SaprkSession
//		//创建SaprkSession
//		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
//		SparkSession spark = SparkSession
//		  .builder()
//		  .appName("Java Spark Hive Example")
//		  .config("spark.sql.warehouse.dir", warehouseLocation)
//		  .enableHiveSupport()
//		  .getOrCreate();
		SparkConf conf =new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(sc.sc());
		
		//创建销售额表，sales
		hiveContext.sql("DROP TABLE IF EXISTS sales");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("
				+"product STRING,"
				+"category STRING,"
				+"revenue BIGINT)");
		hiveContext.sql("LOAD DATA "
				+"LOCAL INPATH '/opt/modules/spark-study/resource/sales.txt' "
				+ "INTO TABLE sales");
		
		//开始编写我们的统计逻辑，使用row_number()开窗函数
		//先说明一下，row_number（）开窗函数的作用
		//其实，就是给每个分组的数据，按照其排序顺序，打上一个分组内的行号
		//比如说，有一个分组date=20151001，里面有三条数据1122,1121,1124
		//那么对这个分组的每一行使用row_number()开窗函数以后，三行，一次会获得一个组内的行号
		//行号 从1开始递增，比如 1122 1,1121 2,1124 3
		Dataset<Row> top3SalesDF = hiveContext.sql(""
				+"SELECT product,category,revenue " 
				+"FROM  ("
					+"SELECT "
						+"product, "
						+"category, "
						+"revenue, "
						//row_number 开窗函数的语法说明
						//首先，可以在select查询时，使用row_number（）函数
						//其次，row_number()函数后面跟上OVER 关键字
						//然后括号，是Partition BY，也就说根据那个字段进行分组
						//其次是可以用ORDER BY进行组内排序
						//然后row_number()就可以给每个组给一个组内行号
						+"row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
						+"FROM sales "
					+ ") tmp_sales "
					+ " where rank<=3");
		
		//将每组排名前三的数据，保存到一个表中
		hiveContext.sql("DROP TABLE IF EXISTS top3_sales");
		
		top3SalesDF.write().saveAsTable("top3_sales");
		
		sc.close();
	}
}
