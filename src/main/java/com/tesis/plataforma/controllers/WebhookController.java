package com.tesis.plataforma.controllers;

import com.tesis.plataforma.services.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;

/**
 * Controlador reactivo WebFlux para recepción de webhooks Git (GitHub / GitLab).
 *
 * =====================================================================
 * ANÁLISIS TÉCNICO: CÓMO WEBFLUX MANEJA LA ASINCRONÍA EN EL WEBHOOK
 * =====================================================================
 *
 * PROBLEMA del enfoque tradicional (Spring MVC + Tomcat):
 *   El hilo HTTP queda bloqueado hasta que Kafka confirme la escritura.
 *   Con alto volumen de webhooks (ej: CI/CD activo), el pool de Tomcat
 *   se agota y nuevas peticiones quedan en cola → latencia acumulada.
 *
 * SOLUCIÓN con WebFlux (Netty + Project Reactor):
 *
 *   Petición HTTP
 *       │
 *       ▼
 *   [Netty Event Loop] ──► NO BLOQUEA
 *       │
 *       ▼ (suscripción en boundedElastic)
 *   Mono.fromRunnable(() -> kafkaProducer.publicar(payload))
 *       │
 *       ▼ (inmediatamente, sin esperar a Kafka)
 *   return 202 Accepted ──► Frontend recibe respuesta en < 5ms
 *       │
 *       ▼ (en paralelo, en otro thread)
 *   Kafka recibe el mensaje ──► Consumer procesa en background
 *
 * Clave: subscribeOn(Schedulers.boundedElastic()) mueve la operación
 * de publicación a Kafka a un thread pool de I/O, dejando libre el
 * event loop de Netty. El HTTP 202 se retorna ANTES de que Kafka
 * confirme la recepción del mensaje.
 *
 * Este patrón se denomina "Fire and Forget reactivo": el webhook confirma
 * recepción instantáneamente y el procesamiento ocurre de forma asíncrona.
 */
@RestController
@RequestMapping("/api/webhooks")
@RequiredArgsConstructor
@Slf4j
public class WebhookController {

    private final KafkaProducerService kafkaProducerService;

    /**
     * POST /api/webhooks/git
     *
     * Endpoint receptor de webhooks de GitHub/GitLab.
     *
     * @param payload  El body JSON del webhook (mapa clave-valor genérico).
     *                 GitHub envía campos como: ref, before, after, repository, commits, pusher.
     *                 GitLab envía: object_kind, checkout_sha, repository, commits.
     *
     * @return Mono<ResponseEntity<Map<String, String>>> con HTTP 202 Accepted.
     *
     * Por qué 202 y no 200:
     *   - 200 OK implica que el procesamiento se completó.
     *   - 202 Accepted semánticamente significa "recibido, será procesado".
     *   - GitHub/GitLab reintentarán el webhook si reciben 5xx o timeout;
     *     el 202 inmediato previene reintentos innecesarios.
     *
     * Por qué Mono<ResponseEntity> y no ResponseEntity directamente:
     *   - WebFlux requiere tipos reactivos en la firma para participar en
     *     el pipeline no bloqueante. Un ResponseEntity plano funcionaría,
     *     pero perdería la capacidad de componer operaciones reactivas.
     */
    @PostMapping(
            value = "/git",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Mono<ResponseEntity<Map<String, String>>> recibirWebhookGit(
            @RequestBody Map<String, Object> payload,
            @RequestHeader(value = "X-GitHub-Event", required = false) String githubEvent,
            @RequestHeader(value = "X-Gitlab-Event", required = false) String gitlabEvent) {

        String tipoEvento = githubEvent != null ? githubEvent : (gitlabEvent != null ? gitlabEvent : "desconocido");

        log.info("[WebhookController] Webhook recibido | tipo: {} | keys: {}",
                tipoEvento, payload.keySet());

        /*
         * NÚCLEO DEL PATRÓN ASÍNCRONO:
         *
         * Mono.fromRunnable(() -> ...) crea un Mono que envuelve una operación void.
         * La lambda NO se ejecuta al crear el Mono; solo se ejecuta cuando alguien
         * se suscribe al pipeline (el framework WebFlux lo hace internamente).
         *
         * subscribeOn(Schedulers.boundedElastic()) delega la ejecución al thread pool
         * de I/O elástico. Este scheduler está optimizado para operaciones potencialmente
         * bloqueantes (llamadas de red, disco), con un límite superior de threads para
         * evitar agotamiento de recursos (bounded = acotado).
         *
         * then(Mono.just(...)) se ejecuta DESPUÉS de que la publicación a Kafka
         * haya sido iniciada (no necesariamente completada), retornando el 202.
         */
        return Mono.fromRunnable(() -> kafkaProducerService.publicarEventoWebhook(payload))
                .subscribeOn(Schedulers.boundedElastic())  // ← Mueve la publicación a Kafka fuera del event loop
                .then(                                      // ← Ejecuta lo siguiente en el pipeline
                        Mono.just(
                                ResponseEntity
                                        .status(HttpStatus.ACCEPTED)       // HTTP 202
                                        .body(Map.of(
                                                "status", "accepted",
                                                "message", "Webhook recibido. Procesamiento en curso.",
                                                "event", tipoEvento
                                        ))
                        )
                )
                .doOnSuccess(r -> log.info("[WebhookController] Respuesta 202 enviada al cliente"))
                .doOnError(e -> log.error("[WebhookController] Error en pipeline del webhook: {}", e.getMessage(), e))
                .onErrorReturn(
                        // En caso de error inesperado, responder 500 en lugar de colgar la conexión
                        ResponseEntity
                                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                                .body(Map.of(
                                        "status", "error",
                                        "message", "Error interno procesando el webhook"
                                ))
                );
    }
}
