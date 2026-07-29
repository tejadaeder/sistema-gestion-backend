package com.tesis.plataforma.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Configuración de Kafka: define los topics que la aplicación necesita.
 *
 * Spring Kafka Admin crea automáticamente los topics declarados como @Bean
 * de tipo NewTopic si no existen en el broker al arrancar la aplicación.
 * Esto es conveniente para desarrollo; en producción los topics suelen
 * crearse de forma manual o por IaC (Terraform, Helm).
 */
@Configuration
public class KafkaConfig {

    /** Nombre del topic inyectado desde application.yml */
    @Value("${app.kafka.topic.git-webhook}")
    private String gitWebhookTopic;

    /**
     * Declara el topic "eventos-git-webhook".
     *
     * - partitions(3): permite paralelismo; 3 consumers pueden procesar en simultáneo.
     * - replicas(1):   adecuado para desarrollo local (broker único).
     *                  En producción usar replicas >= 2 para tolerancia a fallos.
     */
    @Bean
    public NewTopic gitWebhookTopic() {
        return TopicBuilder.name(gitWebhookTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
