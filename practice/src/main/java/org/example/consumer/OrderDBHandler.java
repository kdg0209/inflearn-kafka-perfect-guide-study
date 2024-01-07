package org.example.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;


public class OrderDBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderDBHandler.class.getName());

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private static final String INSERT_ORDER_SQL = "INSERT INTO orders (ord_id, shop_id, menu_name, user_name, phone_number, address, order_time) values (?, ?, ?, ?, ?, ?, ?)";

    public OrderDBHandler(String driver, String url, String user, String password) {
        try {
            Class.forName(driver);
            this.connection = DriverManager.getConnection(url, user, password);
            this.preparedStatement = this.connection.prepareStatement(INSERT_ORDER_SQL);
        } catch(SQLException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
        }
    }

    public void insertOrder(OrderDTO orderDTO)  {
        try {
            PreparedStatement pstmt = this.connection.prepareStatement(INSERT_ORDER_SQL);
            pstmt.setString(1, orderDTO.orderId());
            pstmt.setString(2, orderDTO.shopId());
            pstmt.setString(3, orderDTO.menuName());
            pstmt.setString(4, orderDTO.userName());
            pstmt.setString(5, orderDTO.phoneNumer());
            pstmt.setString(6, orderDTO.address());
            pstmt.setTimestamp(7, Timestamp.valueOf(orderDTO.orderTime()));

            pstmt.executeUpdate();
        } catch(SQLException e) {
            LOGGER.error(e.getMessage());
        }
    }

    public void insertOrders(List<OrderDTO> orders) {
        try {
            PreparedStatement pstmt = this.connection.prepareStatement(INSERT_ORDER_SQL);
            for(OrderDTO orderDTO : orders) {
                pstmt.setString(1, orderDTO.orderId());
                pstmt.setString(2, orderDTO.shopId());
                pstmt.setString(3, orderDTO.menuName());
                pstmt.setString(4, orderDTO.userName());
                pstmt.setString(5, orderDTO.phoneNumer());
                pstmt.setString(6, orderDTO.address());
                pstmt.setTimestamp(7, Timestamp.valueOf(orderDTO.orderTime()));

                pstmt.executeUpdate();
            }
        } catch(SQLException e) {
            LOGGER.info(e.getMessage());
        }
    }

    public void close() {
        try {
            LOGGER.info("###### OrderDBHandler is closing");
            this.preparedStatement.close();
            this.connection.close();
        }catch(SQLException e) {
            LOGGER.error(e.getMessage());
        }
    }
}
