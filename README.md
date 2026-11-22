# ms-rutaexpress-kafka-admin

> Repositorio `ms-rutaexpress-audit`. La aplicación se llama `kafka-admin`.

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

## Infraestructura con Docker Compose

Este repositorio incluye la arquitectura Kafka del proyecto:

| Archivo | Levanta |
| --- | --- |
| `docker-compose.yml` | 1 Zookeeper, 3 brokers Kafka (`kafka-1..3`), creación de los tópicos y, con el perfil `app`, este microservicio |
| `docker-compose.kafka-ui.yml` | Kafka-UI (Kafbat UI) para administrar brokers y tópicos |

```powershell
# 1. Zookeeper, 3 brokers y tópicos
docker compose up -d
docker compose ps        # kafka-init-topics debe terminar con "Exited (0)"

# 2. Kafka-UI en http://localhost:8089
docker compose -f docker-compose.kafka-ui.yml up -d

# 3. (Opcional) kafka-admin en contenedor, en http://localhost:8090
docker compose --profile app up -d --build
```

Detalles de la arquitectura:

- Zookeeper gestiona los metadatos del clúster; los brokers 1, 2 y 3 se registran en él.
- Cada broker tiene dos listeners: `INTERNAL` (`kafka-N:29092`) para la red Docker `rutaexpress-kafka` y `EXTERNAL` (`localhost:9092`, `9093`, `9094`) para clientes fuera de Docker, como IntelliJ.
- Replicación por defecto 3 y `min.insync.replicas=2`: el clúster sigue aceptando escrituras con un broker caído.
- `auto.create.topics.enable=false`: los tópicos se crean con `kafka-init-topics` o con esta API.
- `kafka-init-topics` crea `rutaexpress.shipments.events.v1`, `rutaexpress.shipments.analytics.v1` y `rutaexpress.shipments.events.v1.DLT`, con 3 particiones y 3 réplicas.
- Los puertos solo escuchan en `127.0.0.1` por defecto. Las variables para otra máquina están en `.env.example`.
- Kafka-UI pide usuario y contraseña: antes del paso 2, copia `.env.example` como `.env` y define `KAFKA_UI_PASSWORD`.

### Despliegue en EC2

Recomendado: Zookeeper, los 3 brokers, Kafka-UI, kafka-admin y kafka-analytics en **una misma EC2**
(mínimo 4 GB de RAM, por ejemplo t3.medium; con 8 GB va holgado), porque comparten la red Docker
`rutaexpress-kafka`. Los demás microservicios (Shipments) se conectan desde sus propias EC2.

1. Clonar `ms-rutaexpress-audit` y `ms-rutaexpress-report` en la EC2.
2. En `ms-rutaexpress-audit`, crear `.env` desde `.env.example` con:
   `KAFKA_EXTERNAL_HOST=<IP privada de esta EC2>`, `KAFKA_BIND_ADDRESS=0.0.0.0`,
   `KAFKA_HEAP_OPTS=-Xms256m -Xmx384m` (si la EC2 tiene 4 GB), `KAFKA_UI_PASSWORD=<tu contraseña>`
   y `KAFKA_UI_BIND_ADDRESS=0.0.0.0`.
3. `docker compose up -d`, `docker compose -f docker-compose.kafka-ui.yml up -d` y
   `docker compose --profile app up -d --build`.
4. En `ms-rutaexpress-report`: `docker compose up -d --build`.
5. Security group de esta EC2: 9092-9094 solo desde el security group de Shipments;
   8089 (Kafka-UI) solo desde tu IP. No abrir 2181 ni 29092.
6. En la EC2 de Shipments, agregar a su `.env`: `KAFKA_ENABLED=true` y
   `KAFKA_BOOTSTRAP_SERVERS=<IP privada>:9092,<IP privada>:9093,<IP privada>:9094`.

Comprobar el clúster:

```powershell
docker exec rutaexpress-kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:29092 | Select-String "id:"
docker exec rutaexpress-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --topic rutaexpress.shipments.events.v1
Invoke-RestMethod http://localhost:8090/api/v1/admin/cluster
```

Detener: `docker compose -f docker-compose.kafka-ui.yml down` y luego `docker compose --profile app down`. Agrega `-v` solo si quieres borrar los datos de Kafka.

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

1. Abrir el `pom.xml` de este repositorio como proyecto Maven.
2. Seleccionar Java 21 como Project SDK.
3. Esperar la importación Maven.
4. Levantar la infraestructura con `docker compose up -d`.
5. Definir `KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094` y `KAFKA_DEFAULT_REPLICATION_FACTOR=3` en la configuración de ejecución.
6. Ejecutar `KafkaAdminApplication`.
7. Consultar `http://localhost:8090/api/v1/admin/cluster`.

## Pruebas

```powershell
.\mvnw.cmd clean verify
```

Las pruebas unitarias verifican también DELETE, errores y timeouts con mocks del cliente. La prueba de integración inicia un Kafka KRaft temporal y ejecuta operaciones reales de creación, consulta, configuración y particionado; no necesita AWS ni un broker instalado.
