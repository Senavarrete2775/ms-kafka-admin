package cl.tnt.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/** Tópicos a garantizar al iniciar, definidos en application.yml (app.kafka.topics). */
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaTopicsProperties(List<TopicDefinition> topics) {

    public record TopicDefinition(String name, int partitions, short replicationFactor,
                                  String cleanupPolicy, long retentionMs, int minInsyncReplicas) {
    }

    public List<TopicDefinition> topics() {
        return topics == null ? List.of() : topics;
    }
}
