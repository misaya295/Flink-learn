package com.misaya.flink.utils;

import com.misaya.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

public class SinkToMySQL extends RichSinkFunction<Student> {


    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        connection = getconnection();
        String sql = "insert into student(id,name,password,age) values(?, ?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();

        }
        if (ps != null) {
            ps.close();

        }
    }


    public void invoke(Student value, Context context) throws Exception {

        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setString(3, value.getPassword());
        ps.setInt(4, value.getAge());
        ps.executeUpdate();


    }

    private static Connection getconnection() throws ClassNotFoundException, SQLException {

        Connection cn = null;
        try {    Class.forName("com.mysql.jdbc.Driver");



            cn = DriverManager.getConnection("jdbc:mysql://localhost:3306/fink-learn?useUnicode=true&characterEncoding=UTF-8", "root", "123456");

        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }

        return cn;


    }







}
