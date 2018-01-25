package cn.spark.study.sql.entity;

import java.io.Serializable;

public class StudentScore implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String name;
	private int age;
	private int score;
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
	public int getScore() {
		return score;
	}
	public void setScore(int score) {
		this.score = score;
	}
	@Override
	public String toString() {
		return "StudentScore [name=" + name + ", age=" + age + ", score="
				+ score + "]";
	}
	
}
