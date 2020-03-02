package com.StreamDemo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

import java.util.Arrays;
import java.util.Properties;

public class Twittercount {

    public static void main(String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "TwitterCount");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> twitterCount = builder.stream("twitterfeeds", Consumed.with(Serdes.String(), jsonSerde));


        // create the initial json object for balances
        ObjectNode initialCount = JsonNodeFactory.instance.objectNode();
        initialCount.put("count", 0);
        //initialCount.put("balance", 0);
        //initialCount.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, Long> newCount = twitterCount
                .mapValues(newCounts -> newCounts.asText())
                .selectKey((key, word) -> word)
                .groupByKey()
                .count(Materialized.as("Counts"));;

        newCount.toStream().to("twittercounts",Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
