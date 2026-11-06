package com.moulionmission.telemetry.config;

import com.moulionmission.telemetry.model.TelemetryDTO;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Value("${telemetry.topic.events:telemetry.events.primary}")
    private String primaryTopic;

    @Value("${telemetry.topic.dlq:telemetry.events.dlq}")
    private String dlqTopic;

    @Value("${telemetry.concurrency.threads:16}")
    private int concurrencyThreads;

    @Bean
    public NewTopic primaryTelemetryTopic() {
        // 6 partitions over 3-broker cluster with replication factor 3
        return TopicBuilder.name(primaryTopic)
                .partitions(6)
                .replicas(3)
                .build();
    }

    @Bean
    public NewTopic dlqTelemetryTopic() {
        return TopicBuilder.name(dlqTopic)
                .partitions(3)
                .replicas(3)
                .build();
    }

    @Bean
    public ThreadPoolTaskExecutor kafkaConsumerExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(concurrencyThreads);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(10000);
        executor.setThreadNamePrefix("kafka-telemetry-worker-");
        executor.initialize();
        return executor;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TelemetryDTO> kafkaListenerContainerFactory(
            ConsumerFactory<String, TelemetryDTO> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate,
            ThreadPoolTaskExecutor kafkaConsumerExecutor) {

        ConcurrentKafkaListenerContainerFactory<String, TelemetryDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrencyThreads);

        // Dead Letter Queue recoverer: routes unprocessable poisoned messages to DLQ topic
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(dlqTopic, record.partition()));

        // Retry 3 times with 1-second backoff before forwarding to Dead Letter Queue
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
