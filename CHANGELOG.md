# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y versionado [SemVer](https://semver.org/lang/es/).

## [Unreleased]

### Agregado
- Microservicio administrador de Kafka: crear, listar, consultar y eliminar tópicos; modificar configuración y particiones.
- Consulta de consumer groups y del estado del clúster.
- Creación automática de `pedidos.eventos` y `pedidos.estado` (3 particiones, 3 réplicas).
- Validación de entradas y respuestas de error uniformes.
- Dockerfile.
