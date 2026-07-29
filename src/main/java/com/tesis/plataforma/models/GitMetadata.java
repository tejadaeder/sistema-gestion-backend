package com.tesis.plataforma.models;

import jakarta.persistence.*;
import lombok.*;

/**
 * Entidad JPA que representa los metadatos Git asociados a una lección.
 * Relaciona un commit específico de un repositorio con la lección que documenta ese cambio.
 *
 * Relación: Muchos GitMetadata pueden pertenecer a una misma Leccion (ManyToOne).
 */
@Entity
@Table(name = "git_metadata")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GitMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_metadata")
    private Long idMetadata;

    /**
     * Relación con la entidad Leccion.
     * LAZY: JPA no carga la lección hasta que se acceda explícitamente
     * (optimización para evitar N+1 queries).
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "id_leccion", nullable = false)
    private Leccion leccion;

    /**
     * Hash SHA-1 del commit de Git que originó el evento (ej: "a3f8c2d1...").
     */
    @Column(name = "commit_hash", nullable = false, length = 100)
    private String commitHash;

    /**
     * URL del repositorio remoto (GitHub/GitLab).
     * Ej: "https://github.com/org/repositorio"
     */
    @Column(name = "repo_url", nullable = false, length = 500)
    private String repoUrl;
}
