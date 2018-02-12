package cn.spark.study.streaming.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * 简易版的连接池
 * @author GYJ
 * 2018-2-12
 */
public class ConnectionPool {

	//静态的connection队列
	private static LinkedList<Connection> connectionQueue;
	
	/**
	 * 加载驱动
	 */
	static{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 获取连接，多线程访问并发控制
	 */
	public synchronized static Connection getConnection(){
		try {
			if(connectionQueue == null){
				connectionQueue = new LinkedList<Connection>();
				for (int i = 0; i < 10; i++) {
					Connection conn = DriverManager.getConnection(
							"jdbc:mysql://spark1:3306/testdb", 
							"", 
							"");
					connectionQueue.push(conn);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		return connectionQueue.poll();
	}
	/**
	 * 还回去一个连接
	 */
	public static void returnConnection(Connection conn) {
		connectionQueue.push(conn);
	}
}
