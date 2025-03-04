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

import DAO.SalesPerday;
import DAO.TotalSalesPerCategory;
import DAO.Transactions;
import Deserializer.JSONValueDeserializationSchema;
import ConfigReader.ConfigReader;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String topic="financial_transactions";
		ConfigReader configs=new ConfigReader();
		String jdbcUrl= configs.get("db.url");
		String username=configs.get("db.username");
		String password=configs.get("db.password");
		String driver=configs.get("db.driver");
		int batchSize= Integer.parseInt(configs.get("db.batch_size"));
		System.out.println("batch_size"+batchSize);
		int batchInterval=Integer.parseInt(configs.get("db.batch_interval"));

		KafkaSource<Transactions> source = KafkaSource.<Transactions>builder()
											.setBootstrapServers("broker:29092")
											.setTopics(topic)
											.setGroupId("FlinkCommerce")
											.setStartingOffsets(OffsetsInitializer.earliest())
											.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
											.build();
		DataStream<Transactions> transactionStream=env.fromSource(source, WatermarkStrategy.noWatermarks(),"kafka source");

		JdbcExecutionOptions options=new JdbcExecutionOptions.Builder()
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
		transactionStream.addSink(JdbcSink.sink(
				" CREATE TABLE IF NOT EXISTS transactions ("+
						                     "transaction_id VARCHAR(255) PRIMARY KEY," +
						                        "product_id VARCHAR(255)," +
						                        "product_name VARCHAR(255)," +
						                        "product_category VARCHAR(255)," +
						                        "product_price DOUBLE PRECISION," +
						                       "product_quantity INTEGER," +
						                        "product_brand VARCHAR(255)," +
						                        "total_amount DOUBLE PRECISION," +
						                        "currency VARCHAR(255)," +
						                        "customer_id VARCHAR(255)," +
						                        "transaction_date TIMESTAMP," +
						                       "payment_method VARCHAR(255)" +
						                        ")",
				(JdbcStatementBuilder<Transactions>) (preparedStatement,transaction)->{

				},
				options,
				connOptions
		)).name("Create Transactions Table Sink");

		transactionStream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_category (" +
						"transaction_date DATE, " +
						"category VARCHAR(255), " +
						"total_sales DOUBLE PRECISION, " +
						"PRIMARY KEY (transaction_date, category)" +
						")",
				(JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

				},
				options,
				connOptions
		)).name("Create Sales Per Category Table");


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
				options,
				connOptions
		)).name("Insert into transactions table sink");

		transactionStream.map(
						transaction -> {
							Date transactionDate = new Date(System.currentTimeMillis());
							String category = transaction.getProductCategory();
							double totalSales = transaction.getTotalAmount();
							return new TotalSalesPerCategory(transactionDate, category, totalSales);
						}
				).keyBy((TotalSalesPerCategory salesCategory)->salesCategory.getCategory())
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
						options,
						connOptions
				)).name("Insert into sales per category table");

		transactionStream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_day (" +
						"transaction_date DATE PRIMARY KEY, " +
						"total_sales DOUBLE PRECISION " +
						")",
				(JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

				},
				options,
				connOptions
		)).name("Create Sales Per Day Table");


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
						options,
						connOptions
				)).name("Insert into sales per day table");

		env.execute("FlinkCommerce");
	}
}
