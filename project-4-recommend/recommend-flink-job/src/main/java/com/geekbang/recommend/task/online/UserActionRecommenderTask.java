package com.geekbang.recommend.task.online;

import com.geekbang.recommend.util.HbaseClient;
import com.geekbang.recommend.function.OnlineRecommendMapFunction;
import com.geekbang.recommend.util.Property;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 基于用户评分行为，对用户进行实时推荐
 */
public class UserActionRecommenderTask {

    public static void userActionRecommender() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = Property.getKafkaProperties("rating");
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("rating", new SimpleStringSchema(), properties));
        dataStream.map((MapFunction<String, String>) input -> {
            String[] tmp = input.split(",");
            String rowkey = tmp[0] + "_" + tmp[1] + "_" + tmp[3];
            System.out.println(rowkey);
            // record user rate info
            //  String msg = userId + "," + productId + "," + score + "," + System.currentTimeMillis() / 1000;
            HbaseClient.putData("rating", rowkey, "p", "productId", tmp[1]);
            HbaseClient.putData("rating", rowkey, "p", "userId", tmp[0]);
            HbaseClient.putData("rating", rowkey, "p", "score", tmp[2]);
            HbaseClient.putData("rating", rowkey, "p", "timestamp", tmp[3]);
            // record user-product info
            HbaseClient.increamColumn("userProduct", tmp[0], "product", tmp[1]);
            return input;
        }).map(new OnlineRecommendMapFunction()).print();
        env.execute();
    }
    public static void main(String[] args) throws Exception {
        userActionRecommender();
    }

}
