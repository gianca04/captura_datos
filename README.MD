
# Arquitectura de Telemetría IIoT: Captura, Procesamiento y Visualización

Este documento expone la arquitectura técnica de la solución IIoT desplegada para la captura de variables en controladores lógicos programables (PLC), su enrutamiento y su persistencia. El diseño obedece a las mejores prácticas de la industria (DevOps y SRE), priorizando baja latencia, alta disponibilidad, y eficiencia en el almacenamiento y lectura de series temporales.

## 1. Flujo de Datos (Data Pipeline)

La topología del sistema se compone de cinco capas fundamentales integradas escalonadamente:

### Capa de Extracción (Edge / Gateway)
Script de Python operando con la biblioteca `snap7` para la comunicación directa a nivel de protocolo con el autómata Siemens. Permite extraer bloques contiguos de memoria que contienen variables de proceso (ej. Caudales, Niveles, Temperaturas) en un único ciclo de red, reduciendo el "overhead" en la tarjeta de comunicaciones del PLC.

### Capa de Mensajería (Broker MQTT)
Las métricas crudas se convierten a JSON y se difunden en un broker MQTT. MQTT proporciona un bus de mensajería asíncrono, liviano y altamente robusto ante interrupciones. Las publicaciones siguen una convención semántica de jerarquía de tópicos: `plc/sensores/<equipo>/<variable>`.

### Capa de Ingesta y Enrutamiento (Telegraf)
Actúa como router y transformador de métricas. Telegraf se suscribe al broker MQTT, procesa los tópicos dinámicos para extraer el árbol de jerarquía como etiquetas (tags indexables) y transforma la carga útil al protocolo de InfluxDB (Influx Line Protocol) para maximizar la compresión y eficiencia de transferencia.

### Capa de Almacenamiento a Largo Plazo (VictoriaMetrics / TSDB)
Motor de base de datos de series temporales (TSDB) de ultra alto rendimiento. Recibe los lotes masivos de escritura mediante el protocolo Push de Influx. A su vez, abstrae y estandariza estos datos para el consumo externo utilizando compatibilidad nativa con el motor y lenguaje de Prometheus (PromQL).

### Capa de Streaming en Tiempo Real (Grafana Live)
Vía paralela configurada sobre Telegraf (`outputs.http`) para eludir el retraso de indexación por disco. Envía fotogramas de datos directamente en el núcleo de Grafana vía HTTP Push. Esta conexión permite suscribir los paneles de visualización a WebSockets (`Live Measurements`), alcanzando latencias sub-segundo requeridas en el sector industrial.

## 2. Estándar de Consulta Analítica: PromQL vs InfluxQL

Respecto a la fase de explotación de datos históricos en Grafana, la arquitectura transita del paradigma tradicional de Influx hacia el estándar establecido por Prometheus.

*   **Ingesta (Push):** La escritura se mantiene bajo InfluxDB Line Protocol. Es insuperable en compresión de red y velocidad de inserción pura para componentes de telemetría (Push-based).
*   **Consulta (Read/Query):** La base de datos VictoriaMetrics es consultada desde Grafana utilizando el Data Source de Prometheus. PromQL (Prometheus Query Language) ofrece operadores vectoriales superiores, análisis predictivo más versátil y representa el estándar absoluto en observabilidad moderna.

Con este enfoque, la multiplicidad de equipos se gestiona mediante indexación de etiquetas en lugar de tablas rígidas distribuidas. 

**Ejemplo de consulta PromQL de producción:**
`plc_sensor_valor{equipo="TT_001", variable="Temp"}`

## 3. Consideraciones Críticas de Arquitectura y Lecciones Aprendidas

1.  **Gestión de Estampa de Tiempo (Timestamping):** Es imperativo que la asignación del tiempo ("timestamp") se delegue al colector final en el destino (Telegraf). Proveer el tiempo local desde el nodo edge (Contenedor LxC o PLC) exige una topología estricta de relojes sincronizados por Stratum NTP. Si ocurre una mínima desviación en ms, subsistemas como Grafana Live descartan los paquetes bajo errores inminentes ("Data outside time range").
2.  **Seguridad Perimetral y Control de Acceso:** El acceso o Push de telemetría hacia servicios API remotos (ej. Grafana Live) opera bajo el principio de menor privilegio. Requiere autenticación delegada explícita (Bearer Token) enlazada a un Service Account de Grafana restringido exclusivamente al rol `Metrics Publisher`.
3.  **Prevención de Explosión de Cardinalidad:** Está contraindicada la ramificación de tablas por unidad física de hardware. Todas las lecturas del ecosistema recaen sobre una métrica maestra (ej. `plc_sensor`). La separación vectorial e individualización de los equipos (`TT_001`, `MOTOR_01`) se solventa estructurando etiquetas (tags) predefinidas, blindando las tablas hash y previniendo el ahogamiento de RAM ("OOM Kills") en la TSDB.