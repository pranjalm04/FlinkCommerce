package DAO;
import lombok.Data;


import java.sql.Timestamp;
@Data
public class Transactions {

       private String transactionId;
       private String productId;
       private String ProductName;
       private String productCategory;
       private double productPrice;
       private int productQuantity;
       private String productBrand;
       private double totalAmount;
       private String currency;
       private String customerId;
       private Timestamp transactionDate;
       private String paymentMethod;

}
