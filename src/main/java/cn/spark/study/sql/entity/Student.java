package cn.spark.study.sql.entity;

import java.io.Serializable;
/**
 * 
 * 学生JavaBean
 * @author GYJ
 * 2017-12-1
 */
public class Student implements Serializable {

	private static final long serialVersionUID = 1L;
	private int id;
	private String name;
	private int age;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "Student [id=" + id + ", name=" + name + ", age=" + age + "]";
	}
    
}