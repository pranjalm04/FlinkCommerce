/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkCommerce;

import DAO.*;
import Deserializer.JSONValueDeserializationSchema;
import ConfigReader.ConfigReader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;

import java.time.Duration;


import java.sql.Date;
import java.time.ZoneId;

public class DataStreamJob {
	private final ConfigReader configs;

	public DataStreamJob()
	{
		this.configs=new ConfigReader();
	}
	public <T extends EventTimestamp> WatermarkStrategy<T> getWatermarkStrategy()
	{
		return WatermarkStrategy
				.<T>forBoundedOutOfOrderness(Duration.ofSeconds(10))
				.withTimestampAssigner((event, timestamp) ->  event.getTimestamp().getTime());
	}
	public Tuple2<JdbcExecutionOptions,JdbcConnectionOptions> getJdbcOptions()
	{

		String jdbcUrl= configs.get("db.url");
		String username=configs.get("db.username");
		String password=configs.get("db.password");
		String driver=configs.get("db.driver");
		int batchSize= Integer.parseInt(configs.get("db.batch_size"));
		System.out.println("batch_size"+batchSize);
		int batchInterval=Integer.parseInt(configs.get("db.batch_interval"));

		JdbcExecutionOptions execOptions=new JdbcExecutionOptions.Builder()
				.withBatchSize(batchSize)
				.withBatchIntervalMs(batchInterval)
				.withMaxRetries(3)
				.build();
		JdbcConnectionOptions connOptions=new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(jdbcUrl)
				.withDriverName(driver)
				.withUsername(username)
				.withPassword(password)
				.build();
		return Tuple2.of(execOptions,connOptions);
	}
	@SuppressWarnings("unchecked")
	public <T> KafkaSource<T> getKafkaSource(String topic, Class<T> cls)
	{
		return KafkaSource.<T>builder()
				.setBootstrapServers("broker:29092")
				.setTopics(topic)
				.setGroupId("FlinkCommerce")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema<T>(cls))
				.build();
	}

	public static void main(String[] args) throws Exception {
		DataStreamJob datastream=new DataStreamJob();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		KafkaSource<Transactions> sourceTransactions=datastream.getKafkaSource(datastream.configs.get("transactions_topic"),Transactions.class);

		KafkaSource<CurrencyExchange> sourceCurrencyExchange=datastream.getKafkaSource(datastream.configs.get("currency_topic"),CurrencyExchange.class);

		DataStream<Transactions> transactionStream=env.fromSource(sourceTransactions, datastream.getWatermarkStrategy(),"Sales Transaction source");

		DataStream<CurrencyExchange> currencyExchangeStream=env.fromSource(sourceCurrencyExchange, datastream.getWatermarkStrategy(),"Currency Exchange source");

		Tuple2<JdbcExecutionOptions,JdbcConnectionOptions> jdbcOpt=datastream.getJdbcOptions();
		JdbcExecutionOptions execOptions=jdbcOpt.f0;
		JdbcConnectionOptions connOptions= jdbcOpt.f1;

		currencyExchangeStream.addSink(JdbcSink.sink(
				"INSERT INTO currency_conversion_rates (from_currency, to_currency, exchange_rate, last_updated)"+
				"VALUES (?, ?, ?, ?)"+
				"ON CONFLICT (from_currency, to_currency)"+
				"DO UPDATE SET "+
				"exchange_rate = EXCLUDED.exchange_rate,"+
				"last_updated = EXCLUDED.last_updated;",
				(JdbcStatementBuilder<CurrencyExchange>) (preparedStatement,currencyEx)->{
					preparedStatement.setString(1,currencyEx.getFromCurrency());
					preparedStatement.setString(2,currencyEx.getToCurrency());
					preparedStatement.setDouble(3,currencyEx.getExchangeRate());
					preparedStatement.setTimestamp(4,currencyEx.getLastUpdated());
				},
				execOptions,
				connOptions
		)).name("currency exchange updates");


		transactionStream.addSink(JdbcSink.sink(
				"INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " +
						"product_quantity, product_brand, total_amount, currency, customer_id, transaction_date, payment_method) " +
						"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
						"ON CONFLICT (transaction_id) DO UPDATE SET " +
						"product_id = EXCLUDED.product_id, " +
						"product_name  = EXCLUDED.product_name, " +
						"product_category  = EXCLUDED.product_category, " +
						"product_price = EXCLUDED.product_price, " +
						"product_quantity = EXCLUDED.product_quantity, " +
						"product_brand = EXCLUDED.product_brand, " +
						"total_amount  = EXCLUDED.total_amount, " +
						"currency = EXCLUDED.currency, " +
						"customer_id  = EXCLUDED.customer_id, " +
						"transaction_date = EXCLUDED.transaction_date, " +
						"payment_method = EXCLUDED.payment_method " +
						"WHERE transactions.transaction_id = EXCLUDED.transaction_id",
				(JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {
					preparedStatement.setString(1, transaction.getTransactionId());
					preparedStatement.setString(2, transaction.getProductId());
					preparedStatement.setString(3, transaction.getProductName());
					preparedStatement.setString(4, transaction.getProductCategory());
					preparedStatement.setDouble(5, transaction.getProductPrice());
					preparedStatement.setInt(6, transaction.getProductQuantity());
					preparedStatement.setString(7, transaction.getProductBrand());
					preparedStatement.setDouble(8, transaction.getTotalAmount());
					preparedStatement.setString(9, transaction.getCurrency());
					preparedStatement.setString(10, transaction.getCustomerId());
					preparedStatement.setTimestamp(11, transaction.getTransactionDate());
					preparedStatement.setString(12, transaction.getPaymentMethod());
				},
				execOptions,
				connOptions
		)).name("Insert into transactions table sink");

		transactionStream.map(
						transaction -> {
							Date transactionDate = new Date(System.currentTimeMillis());
							String category = transaction.getProductCategory();
							double totalSales = transaction.getTotalAmount();
							return new TotalSalesPerCategory(transactionDate, category, totalSales);
						}
				).keyBy(TotalSalesPerCategory::getCategory)
				.reduce((salesPerCategory, t1) -> {
					salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
					return salesPerCategory;
				}).addSink(JdbcSink.sink(
						"INSERT INTO sales_per_category(transaction_date, category, total_sales) " +
								"VALUES (?, ?, ?) " +
								"ON CONFLICT (transaction_date, category) DO UPDATE SET " +
								"total_sales = EXCLUDED.total_sales " +
								"WHERE sales_per_category.category = EXCLUDED.category " +
								"AND sales_per_category.transaction_date = EXCLUDED.transaction_date",
						(JdbcStatementBuilder<TotalSalesPerCategory>) (preparedStatement, salesPerCategory) -> {
							preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
							preparedStatement.setString(2, salesPerCategory.getCategory());
							preparedStatement.setDouble(3, salesPerCategory.getTotalSales());
						},
						execOptions,
						connOptions
				)).name("Insert into sales per category table");


		transactionStream.map(
				(Transactions transaction)-> {
					Date transactionDate = Date.valueOf(transaction.getTransactionDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
					Double total_amount = transaction.getTotalAmount();
					return new SalesPerday(transactionDate, total_amount);
				}).keyBy(SalesPerday::getTransactionDate)
						.reduce((SalesPerday d1,SalesPerday d2)->{
							d1.setTotalSalesPerDay(d1.getTotalSalesPerDay()+d2.getTotalSalesPerDay());
							return d1;
						})
						.addSink(JdbcSink.sink(
						"INSERT INTO sales_per_day(transaction_date, total_sales) " +
								"VALUES (?, ?) " +
								"ON CONFLICT (transaction_date) DO UPDATE SET " +
								"total_sales = EXCLUDED.total_sales " +
								"WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date",
						(JdbcStatementBuilder<SalesPerday>) (preparedStatement, salesPerday) -> {
							preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
							preparedStatement.setDouble(2, salesPerday.getTotalSalesPerDay());
						},
						execOptions,
						connOptions
				)).name("Insert into sales per day table");

		env.execute("FlinkCommerce");
	}
}
