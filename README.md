# ms-rutaexpress-kafka-admin

Microservicio administrativo de RutaExpress para consultar el clúster y gestionar tópicos, particiones y configuraciones mediante la API oficial `org.apache.kafka.clients.admin.Admin`.

## Tecnologías

- Java 21
- Spring Boot 4.1.2
- Spring Web MVC y Validation
- Spring for Apache Kafka
- Apache Kafka AdminClient
- Actuator
- Maven Wrapper

## Responsabilidad

Este servicio administra infraestructura Kafka. No publica eventos de negocio ni genera estadísticas; esas responsabilidades permanecen en `kafka-analytics`.

Los topics de Analytics conocidos por defecto son:

- `rutaexpress.shipments.events.v1`
- `rutaexpress.shipments.analytics.v1`
- `rutaexpress.shipments.events.v1.DLT`

Se pueden consultar y modificar sus configuraciones permitidas, pero están protegidos contra eliminación accidental.

## Arquitectura

```text
REST /api/v1/admin
        |
        v
KafkaAdminController
        |
        v
KafkaAdministrationService
  validaciones / políticas
        |
        v
AdminClientKafkaOperations
  timeout / excepciones
        |
        v
Apache Kafka AdminClient
```

## Configuración externa

| Variable | Predeterminado local | Descripción |
| --- | --- | --- |
| `SERVER_PORT` | `8090` | Puerto HTTP |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers separados por coma |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Protocolo Kafka |
| `KAFKA_REQUEST_TIMEOUT` | `10s` | Timeout máximo de operaciones |
| `KAFKA_DEFAULT_PARTITIONS` | `3` | Particiones para creación cuando no se informan |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | `1` | Réplicas por defecto |
| `KAFKA_ADMIN_WRITE_ENABLED` | `true` | Habilita POST, PATCH y DELETE |
| `KAFKA_ANALYTICS_TOPICS` | Topics de Analytics | Topics mostrados en información de clúster |
| `KAFKA_PROTECTED_TOPICS` | Topics de Analytics e internos | Topics que DELETE no puede eliminar |

No se guardan credenciales. Para AWS se deberá incorporar la autenticación concreta elegida por infraestructura y desplegar la API en una red privada o detrás de autenticación. Hasta entonces, `KAFKA_ADMIN_WRITE_ENABLED=false` deshabilita las mutaciones.

## Endpoints

Base: `/api/v1/admin`

| Método | Ruta | Resultado |
| --- | --- | --- |
| `GET` | `/cluster` | ID, controller, brokers y topics |
| `GET` | `/topics` | Lista de topics, incluidos internos |
| `GET` | `/topics/{topicName}` | Particiones, líder, réplicas e ISR |
| `POST` | `/topics` | Crea un topic |
| `GET` | `/topics/{topicName}/configs` | Configuraciones; valores sensibles redactados |
| `PATCH` | `/topics/{topicName}/configs` | Modifica configuraciones mediante `incrementalAlterConfigs` |
| `PATCH` | `/topics/{topicName}/partitions` | Aumenta el total de particiones |
| `DELETE` | `/topics/{topicName}` | Elimina un topic no protegido |

La consulta de particiones se integra en `GET /topics/{topicName}` para devolver las asignaciones completas en una sola respuesta.

## Ejemplos

Crear un topic:

```powershell
$body = @{
    name = 'rutaexpress.test.events.v1'
    partitions = 3
    replicationFactor = 1
    configs = @{
        'retention.ms' = '86400000'
        'cleanup.policy' = 'delete'
        'min.insync.replicas' = '1'
    }
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri http://localhost:8090/api/v1/admin/topics `
    -ContentType 'application/json' `
    -Body $body
```

Aumentar particiones:

```powershell
Invoke-RestMethod `
    -Method Patch `
    -Uri http://localhost:8090/api/v1/admin/topics/rutaexpress.test.events.v1/partitions `
    -ContentType 'application/json' `
    -Body '{"totalPartitions":6}'
```

Modificar configuraciones:

```powershell
Invoke-RestMethod `
    -Method Patch `
    -Uri http://localhost:8090/api/v1/admin/topics/rutaexpress.test.events.v1/configs `
    -ContentType 'application/json' `
    -Body '{"configs":{"retention.ms":"172800000","cleanup.policy":"delete"}}'
```

## Validaciones administrativas

- Los nombres aceptan letras, números, `.`, `_` y `-`, con máximo de 249 caracteres.
- No se pueden crear topics del espacio interno `__*`.
- El factor de replicación no puede superar los brokers disponibles.
- Las particiones solo pueden aumentar.
- `min.insync.replicas` no puede superar el factor de replicación del topic.
- Solo pueden modificarse `retention.ms`, `retention.bytes`, `cleanup.policy` y `min.insync.replicas`.
- No existe eliminación masiva.
- Los topics configurados como protegidos y los internos no se pueden eliminar.
- Toda operación AdminClient tiene un timeout.

## Respuestas HTTP

- `200`: consulta o modificación exitosa.
- `201`: topic creado.
- `204`: topic eliminado.
- `400`: solicitud o configuración inválida.
- `404`: topic inexistente.
- `409`: topic duplicado, protegido o conflicto de particiones.
- `500`: error interno no controlado.
- `503`: Kafka no disponible o timeout.

## IntelliJ IDEA

1. Abrir `C:\Users\alexa\OneDrive\Documentos\kafka\kafka-admin\pom.xml` como proyecto Maven.
2. Seleccionar Java 21 como Project SDK.
3. Esperar la importación Maven.
4. Levantar la infraestructura Kafka externa.
5. Definir las variables necesarias en la configuración de ejecución.
6. Ejecutar `KafkaAdminApplication`.
7. Consultar `http://localhost:8090/api/v1/admin/cluster`.

## Pruebas

```powershell
.\mvnw.cmd clean verify
```

Las pruebas unitarias verifican también DELETE, errores y timeouts con mocks del cliente. La prueba de integración inicia un Kafka KRaft temporal y ejecuta operaciones reales de creación, consulta, configuración y particionado; no necesita AWS ni un broker instalado.
