package com.tesis.plataforma.models;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * Entidad JPA que representa una lección de conocimiento.
 * Mapea la tabla "leccion" en la base de datos gestion_conocimiento_db.
 *
 * Lombok genera automáticamente: constructor vacío, constructor completo,
 * getters, setters, equals, hashCode y toString, eliminando ~80 líneas de boilerplate.
 */
@Entity
@Table(name = "lecciones")
@Data                   // Genera getters, setters, toString, equals y hashCode
@NoArgsConstructor      // Constructor sin argumentos (requerido por JPA)
@AllArgsConstructor     // Constructor con todos los argumentos
@Builder                // Patrón Builder para construcción fluida de objetos
public class Leccion {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_leccion")
    private Long idLeccion;

    // ---> AGREGAMOS ESTE BLOQUE <---
    @Column(name = "id_usuario", nullable = false)
    private Long idUsuario;

    /**
     * Título descriptivo del error o concepto documentado.
     * Ej: "NullPointerException en servicio de autenticación"
     */
    @Column(name = "titulo_error", nullable = false, length = 255)
    private String tituloError;

    /**
     * Contexto en el que ocurrió el error: entorno, versión, condiciones previas.
     */
    @Column(name = "contexto", columnDefinition = "TEXT")
    private String contexto;

    /**
     * Descripción detallada de la solución aplicada.
     */
    @Column(name = "solucion", columnDefinition = "TEXT")
    private String solucion;

    /**
     * Fecha y hora en que se registró la lección.
     * Se asigna automáticamente antes de persistir si no se proporciona.
     */
    @Column(name = "fecha_creacion", updatable = false)
    private LocalDateTime fechaCreacion;

    /** Asigna la fecha actual al persistir por primera vez. */
    @PrePersist
    protected void onCreate() {
        if (this.fechaCreacion == null) {
            this.fechaCreacion = LocalDateTime.now();
        }
    }

    /**
     * Hash del commit de Git asociado (ej: a1b2c3d4e5f67890).
     */
    @Column(name = "commit_hash", length = 40)
    private String commitHash;

    /**
     * Mensaje del commit recibido desde el Webhook.
     */
    @Column(name = "commit_mensaje", length = 500)
    private String commitMensaje;

    /**
     * Estado del enlace con el control de versiones.
     */
    @Column(name = "enlazado_git")
    private Boolean enlazadoGit = false;
}
