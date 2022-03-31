package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaStreamsChallenge {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-challenge-3");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka

        KStream<String, String> textLines = builder.stream("kafka-challenge-3");
        KTable<String, String> wordCounts =
            textLines
                .filter((key, textLine) -> jsonToChallengeInputBool(textLine))
                .mapValues(textLine -> {
                    ChallengeInput input = jsonToChallengeInput(textLine);
                    return challengeOutputToJson(new ChallengeOutput(input.getScore(), input.getScore(), input.getClientName(), input.getClientCountry(), 1));
                })
                .groupByKey()
                .reduce((v1, v2) -> {
                    ChallengeOutput challengeOutput1 = jsonToChallengeOutput(v1);

                    return challengeOutputToJson(new ChallengeOutput(
                        challengeOutput1.getLastScore(),
                        (challengeOutput1.getLastScore() + challengeOutput1.getAverageScore()) / (challengeOutput1.getCount() + 1),
                        challengeOutput1.getClientName(),
                        challengeOutput1.getClientCountry(),
                        challengeOutput1.getCount() + 1));
                });

        // 7 - to in order to write the results back to kafka
        wordCounts.toStream().to("kafka-challenge-3-output", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();


        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


    private static ChallengeOutput jsonToChallengeOutput(String json) {
        try {
            return objectMapper.readValue(json, ChallengeOutput.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static ChallengeInput jsonToChallengeInput(String json) {
        try {
            return objectMapper.readValue(json, ChallengeInput.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean jsonToChallengeInputBool(String json) {
        try {
            objectMapper.readValue(json, ChallengeInput.class);
            return true;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String challengeOutputToJson(ChallengeOutput output) {
        try {
            return objectMapper.writeValueAsString(output);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}


class ChallengeInput {
    int score;
    String clientName;
    String clientCountry;

    public ChallengeInput() {}

    public ChallengeInput(int score, String clientName, String clientCountry) {
        this.score = score;
        this.clientName = clientName;
        this.clientCountry = clientCountry;
    }

    public int getScore() {
        return score;
    }

    public String getClientName() {
        return clientName;
    }

    public String getClientCountry() {
        return clientCountry;
    }

    @Override
    public String toString() {
        return "ChallengeInput{" +
            "score=" + score +
            ", clientName='" + clientName + '\'' +
            ", clientCountry='" + clientCountry + '\'' +
            '}';
    }
}

class ChallengeOutput {
    int lastScore;
    int averageScore;
    String clientName;
    String clientCountry;
    int count;

    public ChallengeOutput() {}

    public ChallengeOutput(int lastScore, int averageScore, String clientName, String clientCountry, int count) {
        this.lastScore = lastScore;
        this.averageScore = averageScore;
        this.clientName = clientName;
        this.clientCountry = clientCountry;
        this.count = count;
    }

    public int getLastScore() {
        return lastScore;
    }

    public int getAverageScore() {
        return averageScore;
    }

    public String getClientName() {
        return clientName;
    }

    public String getClientCountry() {
        return clientCountry;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "{" +
            "lastScore:" + lastScore +
            ", averageScore:" + averageScore +
            ", clientName:'" + clientName + '\'' +
            ", clientCountry:'" + clientCountry + '\'' +
            ", count:'" + count + '\'' +
            '}';
    }
}
