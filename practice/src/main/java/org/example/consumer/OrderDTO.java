package org.example.consumer;

import java.time.LocalDateTime;

public record OrderDTO(
         String orderId,
         String shopId,
         String menuName,
         String userName,
         String phoneNumer,
         String address,
         LocalDateTime orderTime) {
}
