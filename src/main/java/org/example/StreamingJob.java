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

package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.aggregator.LastDoubleValue;
import org.example.aggregator.LastIntegerValue;
import org.example.aggregator.LastStringValue;
import org.example.deserialization.EventMessage;
import org.example.deserialization.EventMessageDeserializationSchema;
import org.example.messaging.RMQLatencySender;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	private static RMQLatencySender rmqLatencySender;

	static {
		try {
			rmqLatencySender = new RMQLatencySender();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
	}

	private static final String EXCHANGE_NAME = "benchmark";
	private static final String QUEUE_NAME = "flinkTest";

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

		// register User-Defined Aggregate Functions (UDAGGs)
		tableEnv.registerFunction("LastIntegerValue", new LastIntegerValue());
		tableEnv.registerFunction("LastDoubleValue", new LastDoubleValue());
		tableEnv.registerFunction("LastStringValue", new LastStringValue());

		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
				.setHost("localhost")
				.setPort(5672)
				.setVirtualHost("/")
				.setUserName("guest")
				.setPassword("guest")
				.build();

		try {
			ConnectionFactory factory = connectionConfig.getConnectionFactory();
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);
			channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");

			final DataStreamSource<EventMessage> rabbitMQStream = env
					.addSource(new RMQSource<>(
							connectionConfig,
							QUEUE_NAME,
							true,
							new EventMessageDeserializationSchema()));

			tableEnv.createTemporaryView("myTable", rabbitMQStream, "transactionID, id, fieldOne, fieldTwo, fieldThree, fieldFour, fieldFive, fieldSix, fieldSeven, fieldEight, fieldNine, number");

			Table queryTable = tableEnv.sqlQuery("SELECT LastStringValue(transactionID), id, LastIntegerValue(fieldOne) AS fieldOne, LastDoubleValue(fieldTwo) AS fieldTwo, LastStringValue(fieldThree) as fieldThree, LastIntegerValue(fieldFour) AS fieldFour, LastDoubleValue(fieldFive) AS fieldFive, LastStringValue(fieldSix) as fieldSix, LastIntegerValue(fieldSeven) AS fieldSeven, LastDoubleValue(fieldEight) AS fieldEight, LastStringValue(fieldNine) as fieldTNine FROM myTable WHERE number = 1 GROUP BY id");

			// conversion of queryTable to a retractStream (true) = insert, (false) = delete
			DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(queryTable, Row.class);
			retractStream.map(new Mapper());
			// retractStream.print();

			try {
				env.execute("Test Job");
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class Mapper implements MapFunction<Tuple2<Boolean, Row>, String> {
		@Override
		public String map(Tuple2<Boolean, Row> booleanRowTuple2) {
			if (booleanRowTuple2.f0) {
				try {
					//rmqTockSender.sendMessage(booleanRowTuple2.f1.toString().substring(0, 36)+","+System.nanoTime());
					rmqLatencySender.sendMessage("tock" + "," + 0 + "," + booleanRowTuple2.f1.toString().substring(0, 36) + "," + System.nanoTime());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return booleanRowTuple2.f1.toString();
		}
	}
}
