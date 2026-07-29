package com.tesis.plataforma.controllers;

import com.tesis.plataforma.models.Leccion;
import com.tesis.plataforma.repositories.LeccionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Controlador reactivo WebFlux para operaciones sobre Lecciones.
 *
 * PATRÓN CLAVE - WebFlux + JPA bloqueante:
 * Spring Data JPA usa JDBC, que es inherentemente bloqueante (no existe R2DBC aquí).
 * Para no bloquear el event loop de Netty, cada llamada JPA se envuelve con:
 *
 *   Mono.fromCallable(() -> operacionJPA())
 *       .subscribeOn(Schedulers.boundedElastic())
 *
 * Schedulers.boundedElastic() es el scheduler diseñado para tareas de I/O bloqueante;
 * ejecuta la operación en un thread pool separado, liberando el hilo de Netty
 * para seguir atendiendo otras peticiones mientras JPA espera respuesta de MySQL.
 */
@RestController
@RequestMapping("/api/lecciones")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "${cors.allowed-origins}")  // Refuerzo a nivel de controlador
public class LeccionController {

    private final LeccionRepository leccionRepository;

    /**
     * GET /api/lecciones
     * Devuelve todas las lecciones como un stream reactivo Flux<Leccion>.
     *
     * Produces: application/json (lista completa al completarse el Flux).
     * Para streaming real usar MediaType.TEXT_EVENT_STREAM_VALUE con datos en vivo.
     *
     * Flujo reactivo:
     *   1. Netty recibe la petición GET en su event loop (hilo no bloqueante).
     *   2. Mono.fromCallable encapsula findAll() como una operación lazy.
     *   3. subscribeOn(boundedElastic) delega la ejecución a un thread pool de I/O.
     *   4. El hilo de Netty queda libre; cuando JPA devuelve la lista, el resultado
     *      se mapea a Flux con flatMapIterable para emitir cada Leccion individualmente.
     *   5. WebFlux serializa el Flux a JSON y escribe la respuesta.
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<Leccion> obtenerTodasLasLecciones() {
        log.debug("[LeccionController] GET /api/lecciones - Iniciando consulta reactiva");

        return Mono.fromCallable(leccionRepository::findAll)  // (1) Encapsula la llamada bloqueante
                .subscribeOn(Schedulers.boundedElastic())      // (2) Ejecuta en thread pool de I/O
                .flatMapIterable(lista -> lista)               // (3) Convierte List<Leccion> en Flux<Leccion>
                .doOnComplete(() -> log.debug("[LeccionController] Stream de lecciones completado"))
                .doOnError(e -> log.error("[LeccionController] Error consultando lecciones: {}", e.getMessage()));
    }

    /**
     * GET /api/lecciones/{id}
     * Devuelve una lección por su ID envuelta en un Mono.
     *
     * Mono<T> = 0 o 1 elemento. Es el equivalente reactivo de Optional<T>.
     * switchIfEmpty emite un 404 si el ID no existe, en lugar de devolver null.
     */
    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<Leccion> obtenerLeccionPorId(@PathVariable Long id) {
        log.debug("[LeccionController] GET /api/lecciones/{}", id);

        return Mono.fromCallable(() -> leccionRepository.findById(id))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(opt -> opt.map(Mono::just)
                        .orElseGet(() -> Mono.error(
                                new RuntimeException("Lección no encontrada con id: " + id)
                        )));
    }

    /**
     * POST /api/lecciones
     * Crea una nueva lección. Devuelve la entidad persistida en un Mono.
     */
    @PostMapping(
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Mono<Leccion> crearLeccion(@RequestBody Leccion leccion) {
        log.info("[LeccionController] POST /api/lecciones - Creando: {}", leccion.getTituloError());

        return Mono.fromCallable(() -> leccionRepository.save(leccion))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
