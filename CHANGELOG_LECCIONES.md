# Bitácora de Lecciones Aprendidas e Incidencias Técnicas

## [Lección #1] - Error de compilación javac por variables de entorno
* **Fecha:** 2026-08-14
* **Categoría:** Configuración de Entorno e Infraestructura Base
* **Problema:** Al ejecutar scripts o comandos en consola, el sistema retorna "javac no se reconoce como comando interno o externo".
* **Solución aplicada:** Se configuró la variable del sistema JAVA_HOME apuntando al JDK 17 y se agregó %JAVA_HOME%\bin a la variable PATH.
* **Validación:** Comprobado en terminal mediante `java -version` y `javac -version`.