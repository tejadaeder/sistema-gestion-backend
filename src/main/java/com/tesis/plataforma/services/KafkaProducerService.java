package com.tesis.plataforma.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Servicio Producer de Kafka.
 *
 * Responsabilidad: serializar el payload del webhook a JSON y publicarlo
 * en el topic "eventos-git-webhook" de forma asíncrona (no bloqueante).
 *
 * El método send() de KafkaTemplate devuelve un CompletableFuture, lo que
 * permite registrar callbacks de éxito/fallo sin bloquear el hilo del caller.
 */
@Service
@RequiredArgsConstructor   // Lombok: constructor con todos los campos final (inyección por constructor)
@Slf4j                     // Lombok: genera el logger 'log' de SLF4J
public class KafkaProducerService {

    /** KafkaTemplate es el componente de Spring para enviar mensajes a Kafka */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /** ObjectMapper de Jackson para serializar objetos a JSON */
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.git-webhook}")
    private String gitWebhookTopic;

    /**
     * Publica el payload del webhook Git en el topic de Kafka.
     *
     * @param payload Mapa que representa el JSON recibido del webhook (GitHub/GitLab).
     *
     * Flujo asíncrono:
     *   1. WebhookController llama a este método en el hilo de Netty (reactivo).
     *   2. Este método serializa el payload y llama a kafkaTemplate.send() que es NO bloqueante.
     *   3. El CompletableFuture registra callbacks; Netty sigue libre para otras peticiones.
     *   4. Cuando Kafka confirma la escritura, se ejecuta el callback de éxito/error.
     */
    public void publicarEventoWebhook(Map<String, Object> payload) {
        try {
            // Serializar el Map a String JSON para enviar como valor del mensaje Kafka
            String payloadJson = objectMapper.writeValueAsString(payload);

            // Clave del mensaje: usar el repo URL como clave garantiza que eventos
            // del mismo repo vayan a la misma partición (orden garantizado por repo)
            String messageKey = extractRepoUrl(payload);

            log.info("[Kafka Producer] Publicando evento en topic '{}' | key: {} | payload: {}",
                    gitWebhookTopic, messageKey, payloadJson);

            // send() es asíncrono: retorna inmediatamente con un CompletableFuture
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(gitWebhookTopic, messageKey, payloadJson);

            // Callbacks no bloqueantes: se ejecutan en el thread pool de Kafka
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("[Kafka Producer] Evento publicado exitosamente | offset: {} | partición: {}",
                            result.getRecordMetadata().offset(),
                            result.getRecordMetadata().partition());
                } else {
                    log.error("[Kafka Producer] Error al publicar evento en Kafka: {}", ex.getMessage(), ex);
                }
            });

        } catch (JsonProcessingException e) {
            log.error("[Kafka Producer] Error serializando payload a JSON: {}", e.getMessage(), e);
            throw new RuntimeException("Error serializando payload del webhook", e);
        }
    }

    /**
     * Extrae la URL del repositorio del payload del webhook para usarla como clave.
     * Tanto GitHub como GitLab incluyen "repository.url" o "repository.html_url".
     */
    @SuppressWarnings("unchecked")
    private String extractRepoUrl(Map<String, Object> payload) {
        try {
            Object repo = payload.get("repository");
            if (repo instanceof Map) {
                Map<String, Object> repoMap = (Map<String, Object>) repo;
                Object url = repoMap.getOrDefault("html_url", repoMap.get("url"));
                if (url != null) return url.toString();
            }
        } catch (Exception e) {
            log.warn("[Kafka Producer] No se pudo extraer repo URL del payload, usando clave genérica");
        }
        return "webhook-sin-repo";
    }
}
