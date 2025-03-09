package DAO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SalesPerday {
    Date transactionDate;
    Double totalSalesPerDay;
}
