# Bitácora de Lecciones Aprendidas e Incidencias Técnicas

## [Lección #1] - Error de compilación javac por variables de entorno
* **Fecha:** 2026-08-14
* **Categoría:** Configuración de Entorno e Infraestructura Base
* **Problema:** Al ejecutar scripts o comandos en consola, el sistema retorna "javac no se reconoce como comando interno o externo".
* **Solución aplicada:** Se configuró la variable del sistema JAVA_HOME apuntando al JDK 17 y se agregó %JAVA_HOME%\bin a la variable PATH.
* **Validación:** Comprobado en terminal mediante `java -version` y `javac -version`.

## [Lección #2] - Conflicto de versiones de dependencias en Maven/Gradle
* **Fecha:** 2026-08-14
* **Categoría:** Gestión de Dependencias y Construcción de Proyectos Backend[cite: 2]
* **Problema:** Excepción java.lang.ClassNotFoundException y NoSuchMethodError por incompatibilidad entre la versión de Spring Boot y librerías externas en pom.xml[cite: 2, 3].
* **Solución aplicada:** Se estandarizaron las versiones dentro del bloque <dependencyManagement> usando el BOM de Spring Boot y se forzó la actualización del repositorio local con `mvn clean install -U`.
* **Validación:**  Empaquetado exitoso del archivo JAR ejecutable sin colisiones de classpath[cite: 2, 3].

## [Lección #3] - Incompatibilidad de versión de Node.js al ejecutar ng serve en Angular CLI
* **Fecha:** 2026-08-14
* **Categoría:** Desarrollo Frontend y Entornos Node/Angular
* **Problema:** Error al iniciar el servidor de desarrollo local mediante `ng serve`: "The Angular CLI requires a minimum Node.js version of v18.13.0", bloqueando el renderizado de la interfaz interactiva.
* **Solución aplicada:** Se instaló Node Version Manager (NVM), se seleccionó la versión compatible mediante `nvm use 18.18.0`, se eliminó el directorio node_modules y se ejecutó `npm install` para regenerar el árbol de dependencias.
* **Validación:** Compilación y despliegue exitoso del servidor de desarrollo en http://localhost:4200 sin advertencias de entorno.