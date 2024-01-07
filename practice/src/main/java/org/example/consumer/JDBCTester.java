package org.example.consumer;

import java.sql.*;

public class JDBCTester {

    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/kafka?serverTimezone=Asia/Seoul&useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "123456789";

    public static void main(String[] args) {
        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            Class.forName(DRIVER);

            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            statement = conn.createStatement();
            resultSet = statement.executeQuery("SELECT * FROM orders; ");

            if (resultSet.next())
                System.out.println(resultSet.getString(1));
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try{
                if(conn != null && !conn.isClosed()){
                    conn.close();
                }
                if(statement != null && !statement.isClosed()){
                    statement.close();
                }
                if(resultSet != null && !resultSet.isClosed()){
                    resultSet.close();
                }
            } catch(SQLException e){
                e.printStackTrace();
            }
        }
    }
}
