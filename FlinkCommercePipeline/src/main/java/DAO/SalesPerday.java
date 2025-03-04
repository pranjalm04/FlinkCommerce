package DAO;
import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;
@Data
@AllArgsConstructor
public class SalesPerday {
    Date transactionDate;
    Double totalSalesPerDay;
}
