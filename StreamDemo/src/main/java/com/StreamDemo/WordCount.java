package com.StreamDemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("WordCountInput");
        KTable<String, Long> wordCounts = textLines
                .mapValues(textLine -> textLine.toLowerCase())
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count(Materialized.as("Counts"));

        //write back to Kafka
        wordCounts.toStream().to("WordCountOutput", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }


    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        String applicationId = "Wordcount";
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCount wordCountApp = new WordCount();

        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);
        streams.start();
        System.out.println(streams.toString());
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
