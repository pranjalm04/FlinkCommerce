package DAO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TotalSalesPerCategory {
    private Date transactionDate;
    private String category;
    private Double totalSales;
}
