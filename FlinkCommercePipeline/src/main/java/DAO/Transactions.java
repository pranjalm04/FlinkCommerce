package DAO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


import java.sql.Timestamp;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transactions implements EventTimestamp{

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

       @Override
       public Timestamp getTimestamp() {
              return this.transactionDate;
       }
}
