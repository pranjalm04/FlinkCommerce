package DAO;
import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;
@Data
@AllArgsConstructor
public class TotalSalesPerCategory {
    private Date transactionDate;
    private String category;
    private Double totalSales;
}
