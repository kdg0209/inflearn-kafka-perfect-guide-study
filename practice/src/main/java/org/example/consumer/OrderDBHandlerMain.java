package org.example.consumer;

import java.time.LocalDateTime;

public class OrderDBHandlerMain {

    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/kafka?serverTimezone=Asia/Seoul&useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "123456789";

    public static void main(String[] args) {
        OrderDBHandler orderDBHandler = new OrderDBHandler(DRIVER, URL, USER, PASSWORD);

        LocalDateTime now = LocalDateTime.now();
        OrderDTO orderDTO = new OrderDTO("ord001", "test_shop", "test_menu", "test_user", "test_phone", "test_address", now);

        orderDBHandler.insertOrder(orderDTO);
        orderDBHandler.close();
    }
}
