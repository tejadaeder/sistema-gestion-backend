package com.tesis.plataforma.repositories;

import com.tesis.plataforma.models.Leccion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repositorio JPA para la entidad Leccion.
 *
 * JpaRepository provee automáticamente los métodos CRUD estándar:
 *   - findAll(), findById(), save(), deleteById(), count(), etc.
 *
 * Spring Data JPA genera la implementación en tiempo de ejecución mediante
 * proxies dinámicos; no se requiere código adicional para las operaciones básicas.
 */
@Repository
public interface LeccionRepository extends JpaRepository<Leccion, Long> {

    /**
     * Consulta derivada: Spring Data traduce el nombre del método a SQL automáticamente.
     * Equivale a: SELECT * FROM leccion WHERE titulo_error LIKE '%keyword%'
     */
    List<Leccion> findByTituloErrorContainingIgnoreCase(String keyword);
}
