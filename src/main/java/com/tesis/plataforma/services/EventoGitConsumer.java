package com.tesis.plataforma.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tesis.plataforma.repositories.LeccionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventoGitConsumer {

    private final LeccionRepository leccionRepository;
    private final ObjectMapper objectMapper; // El mismo Jackson ObjectMapper de tu Producer

    /**
     * Escucha el tópico como String (JSON serializado por tu KafkaProducerService)
     * y actualiza la lección correspondiente en MySQL.
     */
    @KafkaListener(topics = "${app.kafka.topic.git-webhook:eventos-git-webhook}", groupId = "grupo-gestion-conocimiento")
    @Transactional
    public void consumirEventoGit(String payloadJson) {
        log.info("[Kafka Consumer] Mensaje JSON recibido del broker: {}", payloadJson);

        try {
            // Deserializamos el String JSON de vuelta a Map<String, Object>
            Map<String, Object> payload = objectMapper.readValue(
                    payloadJson, new TypeReference<Map<String, Object>>() {}
            );

            // 1. Extraemos el ID de la lección
            Object idObj = payload.get("idLeccion");
            if (idObj == null) {
                log.warn("[Kafka Consumer] El payload no contiene 'idLeccion'. Se descarta el mensaje.");
                return;
            }

            Long idLeccion = Long.valueOf(idObj.toString());
            String hash = payload.get("commitHash") != null ? payload.get("commitHash").toString() : "N/A";
            String mensaje = payload.get("commitMensaje") != null ? payload.get("commitMensaje").toString() : "Commit desde Webhook";

            // 2. Actualizamos la entidad en MySQL
            leccionRepository.findById(idLeccion).ifPresentOrElse(
                    leccion -> {
                        leccion.setCommitHash(hash);
                        leccion.setCommitMensaje(mensaje);
                        leccion.setEnlazadoGit(true);

                        leccionRepository.save(leccion);
                        log.info("[Kafka Consumer] ¡ÉXITO TOTAL! Lección #{} enlazada exitosamente al commit '{}'", idLeccion, hash);
                    },
                    () -> log.error("[Kafka Consumer] No existe en la base de datos la lección #{}", idLeccion)
            );

        } catch (Exception e) {
            log.error("[Kafka Consumer] Error procesando el mensaje JSON de Kafka: {}", e.getMessage(), e);
        }
    }
}