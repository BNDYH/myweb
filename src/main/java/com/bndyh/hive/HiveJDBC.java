package com.bndyh.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class HiveJDBC {
	private static String Driver="org.apache.hive.jdbc.HiveDriver";
	private static String URL="jdbc:hive2://192.168.4.185:10000/default";
	private static String userName="";
	private static String passWord="";
	public static void main(String[] args) {
		try {
			System.out.println("start");
			Class.forName(Driver);
			Connection conn = DriverManager.getConnection(URL, userName, passWord);
			System.out.println("lianjie");
			Statement statement = conn.createStatement();
			String sql="show tables;";
			System.out.println(sql);
			ResultSet rs = statement.executeQuery(sql);
			System.out.println(1231);
			while (rs.next()) {
				System.out.println(rs.toString());
			}
			System.out.println(3245234);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
