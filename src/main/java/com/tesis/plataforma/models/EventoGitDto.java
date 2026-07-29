package com.tesis.plataforma.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventoGitDto {
    private Long idLeccion;      // ID de la lección a enlazar (#3 en tu prueba)
    private String commitHash;   // Hash largo o corto (ej: "a1b2c3d4e5f67890")
    private String commitMensaje;// Mensaje del commit
    private String autor;        // Nombre del desarrollador
}
