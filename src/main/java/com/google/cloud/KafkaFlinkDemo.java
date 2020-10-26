// Based on https://min-api.cryptocompare.com/documentation/websockets?key=Channels&cat=Trade
// SCHEMA of incoming JSON mesages:
//
//  TYPE string Always
//    Type of the message, this is 0 for trade type messages.
//  M string Always
//    The market / exchange you have requested (name of the market / exchange e.g. Coinbase, Kraken, etc.)
//  FSYM string Always
//    The mapped from asset (base symbol / coin) you have requested (e.g. BTC, ETH, etc.)
//  TSYM string Always
//    The mapped to asset (quote/counter symbol/coin) you have requested (e.g. BTC, USD, etc.)
//  F string Always
//    The flag for the trade as a bitmask: &1 for SELL, &2 for BUY, &4 for UNKNOWN and &8 for REVERSED (inverted). A flag of 1 would be a SELL, a flag of 9 would be a SELL + the trade was REVERSED (inverted). We reverse trades when we think the dominant pair should be on the right hand side of the trade. Uniswap for example has ETH trading into a lot of symbols, we record it as the other symbols trading into ETH and we invert the trade. We only use UNKNOWN when the underlying market / exchange API does not provide a side
//  ID string
//    The trade id as reported by the market / exchange or the timestamp in seconds + 0 - 999 if they do not provide a trade id (for uniqueness under the assumption that there would not be more than 999 trades in the same second for exchanges that do not provide a trade id)
//  TS timestamp
//    The timestamp in seconds as reported by the market / exchange or the received timestamp if the market / exchange does not provide one.
//  Q number
//    The from asset (base symbol / coin) volume of the trade (for a BTC-USD trade, how much BTC was traded at the trade price)
//  P number
//    The price in the to asset (quote / counter symbol / coin) of the trade (for a BTC-USD trade, how much was paid for one BTC in USD)
//  TOTAL number
//    The total volume in the to asset (quote / counter symbol / coin) of the trade (it is always Q * P so for a BTC-USD trade, how much USD was paid in total for the volume of BTC traded)
//  RTS timestamp
//    The timestamp in seconds when we received the trade. This varies from a few millisconds from the trade taking place on the market / exchange to a few seconds depending on the market / exchange API options / rate limits
//  CCSEQ number
//    Our internal sequence number for this trade, this is unique per market / exchange and trading pair. Should always be increasing by 1 for each new trade we discover, not in chronological order, only available for a subset of markets / exchanges.
//  TSNS number
//    The nanosecond part of the reported timestamp, only available for a subset of markets / exchanges
//  RTSNS number
//    The nanosecond part of the received timestamp, only available for a subset of markets / exchanges

package com.google.cloud;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Properties;

/*
               Required output: Last 5 mins moving window volume and total traded amount for BTC-USD SELL events
               Window size: 5 mins
               Sliding every 5 secs
                Filter condition:
                1. FSYM = BTC and TSYM = USD and F != 9
                2. FSYM = USD and TSYM = BTC and F == 9
                Output:
                Total volume: Add all Q values for qualifying events
                Total sum: Add all TOTAL fields for qualifying events
*/
public class KafkaFlinkDemo {

    /**
     * TotalVolume class implements methods required for the aggregate functions to
     * calculate the total traded volumes in the provided window
     */
    private static class TotalVolume implements AggregateFunction<ObjectNode, Tuple2<Long, Long>, Tuple2<String, Long>> {

        final ExecutionConfig conf = StreamExecutionEnvironment.getExecutionEnvironment().getConfig();
        final Map<String, String> globalConfigMap = conf.getGlobalJobParameters().toMap();

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(ObjectNode jsonNode, Tuple2<Long, Long> accumulator) {
            String key1Label = globalConfigMap.getOrDefault("key_1_tot_label", "TOTAL");
            Long quantity = 0L;
            if(jsonNode.get(key1Label) != null)
                quantity = jsonNode.get(key1Label).asLong();
            return new Tuple2<>(++accumulator.f0, accumulator.f1 + quantity);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<Long, Long> acc) {
            String keyStr1 = globalConfigMap.getOrDefault("key_1", "BTC-USD-SELL");
            Long averageResult = 0L;
            if (acc.f0 > 0)
                averageResult = acc.f1 / acc.f0;
            return new Tuple2<>(keyStr1, averageResult);
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }

    private static class RedisExampleMapper implements RedisMapper<Tuple2<String, Long>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Long> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Long> data) {
            return data.f1.toString();
        }
    }

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final int windowSize = params.getInt("window", 5*60); // 5 mins
        final int slideSize = params.getInt("slide", 5); // 5 secs
        final String kafkaBootstrapServers = params.get("kafka_bootstrap_servers", "localhost:9092");
        final String kafkaGroupId = params.get("kafka_group_id", "test");
        final String kafkaTopic = params.get("kafka_topic", "crypto-trade");
        final  String flinkHost = params.get("flink_host", "127.0.0.1");
//        Ensure this is documented: params.get("key_1", "BTC-USD-SELL")
        //        Ensure this is documented: params.get("key_1_vol_label", "Q")
        //        Ensure this is documented: params.get("key_1_tot_label", "TOTAL")

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        properties.setProperty("group.id", kafkaGroupId);


        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(flinkHost).build();


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        // get input data
        DataStreamSource<ObjectNode> incomingStream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, new JsonNodeDeserializationSchema(), properties));


        // print() will write the contents of the stream to the TaskManager's standard out stream
        // the rebelance call is causing a repartitioning of the data so that all machines
        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
        AllWindowedStream<ObjectNode, TimeWindow> commonStream = incomingStream.rebalance()
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideSize)));

        SingleOutputStreamOperator<Tuple2<String, Long>> aggVolStream = commonStream.aggregate(new TotalVolume());
        aggVolStream.print();
        aggVolStream.addSink(new RedisSink<>(conf, new RedisExampleMapper()));

        // execute program
        env.execute("crypto-trade");
    }
}