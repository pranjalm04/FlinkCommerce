package FlinkCommerce;

import ConfigReader.ConfigReader;
import DAO.Transactions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class currencyConverter extends RichAsyncFunction<Transactions,Transactions> {
    private transient ExecutorService executor;
    private transient HikariDataSource dataSource;
    private final String databaseUrl;
    private final String password;
    private final String username;
    private static final Logger logger = LoggerFactory.getLogger(currencyConverter.class);
    public currencyConverter(String databaseUrl,String username,String password)
    {
        this.databaseUrl=databaseUrl;
        this.username=username;
        this.password=password;
    }
    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        try {
            logger.info("Configuring Hikari paramaters");
            HikariConfig configs = new HikariConfig();
            configs.setJdbcUrl(this.databaseUrl);
            configs.setUsername(this.username);
            configs.setPassword(this.password);
            this.dataSource = new HikariDataSource(configs);
            this.executor = Executors.newFixedThreadPool(20);
        }
        catch(Exception expection)
        {
           logger.error("Error opening currencyConverter",expection.getMessage());
           throw expection;
        }
    }
    @Override
    public void asyncInvoke(Transactions transactions, ResultFuture<Transactions> resultFuture)
    {
        logger.debug("Async invoke called for transaction: {}", transactions);
        CompletableFuture.supplyAsync(()->{
            try{
                logger.debug("Fetching exchange rate for currency: {}", transactions.getCurrency());
                double exchangeRate= fetchExchangeRate(transactions.getCurrency());
                double amountConvertedToUsd=transactions.getTotalAmount()*exchangeRate;
                transactions.setTotalAmount(amountConvertedToUsd);
                logger.debug("Set the converted amount in transactions");
                return transactions;
            }
            catch (Exception e){
                logger.error("Error during Exchange rate lookup ",e);
                return null;
            }
            },executor)
            .thenAccept(( res)->{
                logger.debug("Completing result future for transaction");
                if(res!=null)
                    resultFuture.complete(Collections.singletonList(res));
                else
                    resultFuture.complete(Collections.singleton(transactions));
            })
            .exceptionally(exception->{
                resultFuture.completeExceptionally(exception);
                return null;
            });


    }
    public double fetchExchangeRate(String currency) throws Exception
    {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement sql = connection.prepareStatement("SELECT exchange_rate FROM currency_conversion_rates WHERE from_currency = ? and to_currency='USD'")) {
            sql.setString(1, currency);
            ResultSet resultSet = sql.executeQuery();
            if (resultSet!=null && resultSet.next()) {
                logger.debug("Exchange rate found:for currency: {}", currency);
                return resultSet.getDouble(1);
            }
            else
                return 1.0;
        }
        catch (SQLException e)
        {
            logger.error("SQLException during exchange rate lookup.", e);
            throw new SQLException(e);
        }

    }
    public void close() throws Exception {
        logger.info("Closing currencyConverter function.");
        if(this.executor!=null)
            this.executor.shutdown();
        if(dataSource!=null)
            this.dataSource.close();
    }
}
