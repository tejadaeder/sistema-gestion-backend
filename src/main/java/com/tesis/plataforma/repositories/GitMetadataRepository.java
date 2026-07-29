package com.tesis.plataforma.repositories;

import com.tesis.plataforma.models.GitMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repositorio JPA para la entidad GitMetadata.
 *
 * Extiende JpaRepository<GitMetadata, Long> donde Long es el tipo del ID (id_metadata).
 */
@Repository
public interface GitMetadataRepository extends JpaRepository<GitMetadata, Long> {

    /**
     * Busca todos los metadatos asociados a una lección específica por su ID.
     * Útil para obtener el historial de commits de una lección.
     */
    List<GitMetadata> findByLeccion_IdLeccion(Long idLeccion);

    /**
     * Busca un registro por su hash de commit único.
     * Permite evitar duplicados al procesar webhooks repetidos.
     */
    Optional<GitMetadata> findByCommitHash(String commitHash);
}
