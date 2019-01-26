package com.narcasse.kafka.dao;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class MySqlClient {

    private Connection getConn() {
        String url = "gp-bd-master01";
        String username = "wuchen";
        String password = "wuchen123456";

        Connection con = null;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (SQLException se) {
            System.out.println("数据库连接失败！");
            se.printStackTrace();
        }

        return con;
    }

    public void insert(String sql) {
        Statement pstmt = null;
        try {
            pstmt = getConn().createStatement();
            pstmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

//    create table test.app_log(
//         id int auto_incremtn primary key,
//            time varchar(128),
//            level varchar(128),
//            thread_name varchar(128),
//            method_name varchar(128),
//            error_msg varchar(128)
//    )

    public void insertLog(Map<String, String> log) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into test.gp_app_log(time,level,thread_name,method_name,error_msg) values (");
        sb.append("'" + log.get("time") + "',");
        sb.append("'" + log.get("level") + "',");
        sb.append("'" + log.get("thread_name") + "',");
        sb.append("'" + log.get("method_name") + "',");
        sb.append("'" + log.get("error_msg") + "'");
        sb.append(" )");
        insert(sb.toString());
    }
}
