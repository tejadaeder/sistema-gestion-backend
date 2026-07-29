package com.tesis.plataforma.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tesis.plataforma.models.GitMetadata;
import com.tesis.plataforma.models.Leccion;
import com.tesis.plataforma.repositories.GitMetadataRepository;
import com.tesis.plataforma.repositories.LeccionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Optional;

/**
 * Servicio Consumer de Kafka.
 *
 * Responsabilidad: escuchar el topic "eventos-git-webhook", procesar el payload
 * del evento Git y persistir los metadatos vinculándolos a una Leccion existente.
 *
 * El procesamiento ocurre en el thread pool de Kafka (@KafkaListener), completamente
 * desacoplado del hilo que atendió la petición HTTP original. Esto es el núcleo
 * del patrón de arquitectura event-driven: el webhook responde en < 5ms (202 Accepted)
 * y el procesamiento pesado ocurre aquí de forma asíncrona.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

    private final ObjectMapper objectMapper;
    private final LeccionRepository leccionRepository;
    private final GitMetadataRepository gitMetadataRepository;

    /**
     * Listener del topic "eventos-git-webhook".
     *
     * @param record       El registro completo de Kafka (incluye key, value, offset, partición).
     * @param acknowledgment Objeto para confirmar manualmente el procesamiento del mensaje.
     *
     * Configuración del listener:
     *   - groupId:    El consumer group al que pertenece esta instancia.
     *   - containerFactory: Usa la fábrica por defecto configurada en application.yml.
     *
     * MANUAL ACK: Con ack-mode: MANUAL_IMMEDIATE, Kafka no avanza el offset hasta que
     * acknowledgment.acknowledge() sea llamado explícitamente. Esto previene la pérdida
     * de mensajes si la aplicación cae durante el procesamiento.
     */
    @KafkaListener(
            topics = "${app.kafka.topic.git-webhook}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional
    public void procesarEventoWebhook(ConsumerRecord<String, String> record,
                                      Acknowledgment acknowledgment) {

        log.info("[Kafka Consumer] Mensaje recibido | topic: {} | partición: {} | offset: {} | key: {}",
                record.topic(), record.partition(), record.offset(), record.key());

        try {
            // Deserializar el JSON del valor del mensaje a un Map
            Map<String, Object> payload = objectMapper.readValue(
                    record.value(),
                    new TypeReference<Map<String, Object>>() {}
            );

            // --- PASO 1: Extraer datos relevantes del payload del webhook ---
            String commitHash = extraerCommitHash(payload);
            String repoUrl    = extraerRepoUrl(payload);

            log.debug("[Kafka Consumer] Procesando evento | commitHash: {} | repoUrl: {}", commitHash, repoUrl);

            // --- PASO 2: Verificar si el commit ya fue procesado (idempotencia) ---
            Optional<GitMetadata> existente = gitMetadataRepository.findByCommitHash(commitHash);
            if (existente.isPresent()) {
                log.warn("[Kafka Consumer] Commit '{}' ya procesado, descartando duplicado.", commitHash);
                acknowledgment.acknowledge();
                return;
            }

            // --- PASO 3: Vincular con una Lección existente (lógica de negocio simulada) ---
            // En un MVP real, esta lógica buscaría la lección por algún identificador
            // incluido en el mensaje del commit (ej: "FIX: LECCION-42 - Resuelve NPE").
            // Aquí se vincula a la primera lección disponible como simulación.
            Optional<Leccion> leccionOpt = leccionRepository.findAll()
                    .stream()
                    .findFirst();

            if (leccionOpt.isEmpty()) {
                log.warn("[Kafka Consumer] No hay lecciones en BD. Se omite persistencia de GitMetadata.");
                acknowledgment.acknowledge();
                return;
            }

            // --- PASO 4: Persistir el GitMetadata vinculado a la Leccion ---
            GitMetadata gitMetadata = GitMetadata.builder()
                    .leccion(leccionOpt.get())
                    .commitHash(commitHash)
                    .repoUrl(repoUrl)
                    .build();

            gitMetadataRepository.save(gitMetadata);

            log.info("[Kafka Consumer] GitMetadata persistido exitosamente | id: {} | leccion: {}",
                    gitMetadata.getIdMetadata(), leccionOpt.get().getIdLeccion());

            // --- PASO 5: Confirmar el offset manualmente (mensaje procesado con éxito) ---
            acknowledgment.acknowledge();

        } catch (JsonProcessingException e) {
            log.error("[Kafka Consumer] Error deserializando mensaje Kafka: {}", e.getMessage(), e);
            // NO se hace acknowledge() para que Kafka reintente según la política configurada
        } catch (Exception e) {
            log.error("[Kafka Consumer] Error inesperado procesando webhook: {}", e.getMessage(), e);
            // En producción: enviar a un Dead Letter Topic (DLT) para análisis posterior
            acknowledgment.acknowledge(); // Acknowledge para no bloquear la partición indefinidamente
        }
    }

    /** Extrae el hash del commit del payload. Funciona con GitHub y GitLab. */
    private String extraerCommitHash(Map<String, Object> payload) {
        // GitHub: campo "after" contiene el SHA del commit más reciente del push
        Object after = payload.get("after");
        if (after != null && !after.toString().isBlank()) {
            return after.toString();
        }
        // GitLab: campo "checkout_sha"
        Object checkoutSha = payload.get("checkout_sha");
        if (checkoutSha != null) return checkoutSha.toString();

        return "HASH_DESCONOCIDO_" + System.currentTimeMillis();
    }

    /** Extrae la URL del repositorio del payload. */
    @SuppressWarnings("unchecked")
    private String extraerRepoUrl(Map<String, Object> payload) {
        try {
            Object repo = payload.get("repository");
            if (repo instanceof Map) {
                Map<String, Object> repoMap = (Map<String, Object>) repo;
                Object url = repoMap.getOrDefault("html_url", repoMap.get("url"));
                if (url != null) return url.toString();
            }
        } catch (Exception e) {
            log.warn("[Kafka Consumer] No se pudo extraer repo URL.");
        }
        return "URL_DESCONOCIDA";
    }
}
