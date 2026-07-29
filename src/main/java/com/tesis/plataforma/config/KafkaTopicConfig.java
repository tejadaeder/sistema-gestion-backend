package com.tesis.plataforma.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Configuración de inicialización de tópicos en el clúster de Kafka.
 * Garante arquitectónico para entornos Docker donde auto.create.topics = false.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String TOPICO_WEBHOOKS = "eventos-git-webhook";

    @Bean
    public NewTopic topicoEventosGit() {
        return TopicBuilder.name(TOPICO_WEBHOOKS)
                .partitions(1)
                .replicas(1) // 1 réplica porque es un clúster local Docker de 1 nodo
                .build();
    }
}