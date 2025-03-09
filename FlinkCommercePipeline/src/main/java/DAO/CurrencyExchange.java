package DAO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;
import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CurrencyExchange implements EventTimestamp{
    private String fromCurrency;
    private String toCurrency;
    private double exchangeRate;
    private Timestamp lastUpdated;
    @Override
    public Timestamp getTimestamp() {
        return this.lastUpdated;
    }
}
