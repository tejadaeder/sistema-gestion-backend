package com.tesis.plataforma.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import java.util.List;

/**
 * Configuración global de CORS para Spring WebFlux.
 *
 * IMPORTANTE: En WebFlux (Netty) se usa CorsWebFilter (reactivo),
 * NO WebMvcConfigurer (que pertenece a Spring MVC / Tomcat).
 * Ambos coexistirían en un proyecto mixto, pero en WebFlux puro
 * solo aplica el enfoque reactivo.
 */
@Configuration
public class CorsConfig {

    /** Origen permitido, inyectado desde application.yml */
    @Value("${cors.allowed-origins}")
    private String allowedOrigin;

    @Bean
    public CorsWebFilter corsWebFilter() {
        CorsConfiguration config = new CorsConfiguration();

        // Permite peticiones desde el frontend Angular en desarrollo
        config.setAllowedOrigins(List.of(allowedOrigin));

        // Métodos HTTP permitidos
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));

        // Headers permitidos en la petición entrante
        config.setAllowedHeaders(List.of("*"));

        // Permite enviar cookies / tokens de autorización en las peticiones
        config.setAllowCredentials(true);

        // Tiempo que el navegador puede cachear la respuesta preflight (segundos)
        config.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        // Aplica la configuración a todos los paths de la API
        source.registerCorsConfiguration("/**", config);

        return new CorsWebFilter(source);
    }
}
