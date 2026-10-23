# Trabajo 1 – Capas Medallion y streaming en AWS

**Curso:** SI7006/SI6003 Almacenamiento y Procesamiento de Grandes Datos – Universidad EAFIT, 2026

**Integrante:** Felipe Martinez Cortes

**Video:** -

---

## Parte A – Capas Medallion con Athena

### Datos
- **Qué es:** datos meteorológicos y de radiación solar **horarios** de **NASA POWER** (API pública), para 15 municipios de Antioquia.
- **Para qué:** son la base del proyecto integrador (evaluación de sistemas solares residenciales en Colombia).
- **Periodo:** 2015-01-01 00:00 a 2024-12-31 23:00.
- **Filas:** 1.315.080 originales (15 municipios × 87.672 horas). No fue necesario aumentar datos.
- **Errores simulados:** la fuente venía limpia (todos los hallazgos en 0), así que, para probar las reglas, se simularon errores típicos de ingesta en ~0,5 % de las filas: 1.500 valores vacíos, 1.200 valores -999, 800 fuera de rango, 500 textos en columnas numéricas, 400 fechas inválidas y 2.000 filas repetidas. El archivo final de bronce tiene **1.317.080 filas**.
- **Peso:** 81,87 MB en CSV (9 columnas).
- **Columnas:**

| Columna | Tipo | Descripción |
|---|---|---|
| municipio | texto | Nombre del municipio |
| latitud, longitud | decimal | Coordenadas del punto |
| fecha_hora | fecha y hora | Hora local (LST) |
| radiacion_wh_m2 | decimal | Radiación solar en superficie (Wh/m²) |
| radiacion_cielo_despejado_wh_m2 | decimal | Radiación si no hubiera nubes (Wh/m²) |
| temperatura_c | decimal | Temperatura a 2 m (°C) |
| humedad_relativa_pct | decimal | Humedad relativa a 2 m (%) |
| velocidad_viento_m_s | decimal | Viento a 10 m (m/s) |

El script de descarga está en `parteA/descargar_datos_nasa.py`. El CSV no se incluye por su tamaño.

### Capas
| Capa | Tabla | Formato |
|---|---|---|
| Bronce | `trabajo1.bronce_nasa` | Tabla externa sobre el CSV en S3, todas las columnas como texto |
| Plata | `trabajo1.plata_validas`, `trabajo1.plata_rechazadas` | Iceberg |
| Oro | `trabajo1.oro_potencial_solar` | Iceberg, agregada por municipio y mes |

Las consultas SQL se ejecutaron en Athena y se muestran en el video.

### Hallazgos
Con los datos originales de NASA todos los hallazgos dieron 0. Con los errores simulados:

| Hallazgo | Filas | % |
|---|---|---|
| Filas totales | 1.317.080 | 100 |
| Filas repetidas exactas | 2.000 | 0,1519 |
| Filas repetidas por municipio + fecha_hora | 2.355 | 0,1788 |
| Filas con algún valor vacío | 1.500 | 0,1139 |
| Fechas con formato inválido | 400 | 0,0304 |
| Valores no numéricos | 500 | 0,0380 |
| Radiación faltante (-999) | 249 | 0,0189 |
| Radiación cielo despejado faltante (-999) | 227 | 0,0172 |
| Temperatura faltante (-999) | 227 | 0,0172 |
| Humedad faltante (-999) | 238 | 0,0181 |
| Viento faltante (-999) | 259 | 0,0197 |
| Valores fuera de rango físico | 800 | 0,0607 |

Las repetidas por clave (2.355) son más que las exactas (2.000) porque las fechas inválidas repiten el mismo texto dentro de un municipio; esas filas se rechazan por R1.

### Reglas de calidad (plata)
| Regla | Qué rechaza |
|---|---|
| R1 | Municipio vacío o fecha inválida |
| R2 | Valor vacío o no numérico |
| R3 | Valor faltante de NASA (-999) |
| R4 | Fuera de rango físico (radiación 0–1400 Wh/m², temperatura −10 a 50 °C, humedad 0–100 %, viento 0–60 m/s) |
| R5 | Fila repetida por municipio + fecha_hora (se conserva una) |

Cada fila se clasifica una sola vez en la vista `bronce_clasificada` (se aplica la primera regla que falle), y de ella salen las dos tablas.

| Motivo | Filas rechazadas |
|---|---|
| R1 | 400 |
| R2 | 2.000 |
| R3 | 1.200 |
| R4 | 800 |
| R5 | 2.000 |

**Cuadre:** bronce 1.317.080 = válidas 1.310.680 + rechazadas 6.400 ✔

### Oro
**Pregunta:** ¿qué municipios de Antioquia, y en qué meses, tienen más potencial solar, y cuánta energía daría al día un sistema residencial de 1 kWp?

- `irradiacion_diaria_kwh_m2`: horas sol pico (HSP).
- `energia_kwh_dia_por_kwp`: HSP × 1 kWp × 0,80 (pérdidas típicas).

**Resultado:** Puerto Berrío tiene el mayor potencial (5,26 HSP, ~1.537 kWh/año por kWp) y Urrao el menor. En casi todos los municipios el mejor mes es agosto y el peor, noviembre. Algunos municipios tienen valores idénticos porque caen en la misma celda de la cuadrícula de NASA POWER.

### Comparación entre capas
Misma consulta en las tres capas: radiación horaria promedio por municipio.

| Capa | Datos escaneados | Tiempo de ejecución |
|---|---|---|
| Bronce (CSV) | 81,87 MB | 1.182 ms |
| Plata (Iceberg / Parquet) | 1,69 MB | 602 ms |
| Oro (Iceberg agregada) | 1,23 KB | 1.500 ms |

**Interpretación:**
- **Bytes:** bajan unas 48 veces de bronce a plata y unas 76.000 veces de bronce a oro. Bronce lee el CSV completo; plata, al guardar por columnas y comprimido, solo lee las 2 columnas necesarias; oro ya está calculada y solo lee 180 filas.
- **Tiempo:** plata es casi el doble de rápida que bronce. Oro no es la más rápida porque, con datos tan pequeños, la mayor parte del tiempo se va en arrancar la consulta y leer los metadatos de Iceberg, y ese tiempo varía de una corrida a otra.
- **Calidad:** el promedio en bronce queda distorsionado por los -999 y los valores fuera de rango; en plata y oro ya no.
- **Costo:** Athena cobra por bytes leídos, así que plata y oro son mucho más baratas.

---

## Parte B – Streaming: IoT Core → SQS → Lambda → DynamoDB

### Flujo
1. `publicar_eventos.py` (en CloudShell) publica eventos JSON en el tema `trabajo1/radiacion` de IoT Core.
2. La regla `trabajo1_a_sqs` (`SELECT * FROM 'trabajo1/radiacion'`) los manda a la cola SQS `cola_eventos_radiacion`.
3. La Lambda `procesar_eventos_radiacion` lee la cola en lotes de 10, valida cada evento y lo guarda.
4. Los eventos válidos van a la tabla `eventos_radiacion`; los descartados, a `eventos_descartados`, con su motivo.

### Esquema de los eventos
```json
{
  "event_id": "uuid",
  "municipio": "Medellin",
  "fecha_hora": "2026-10-01 12:00",
  "radiacion_wh_m2": 650.5,
  "temperatura_c": 24.3,
  "humedad_relativa_pct": 70.1,
  "velocidad_viento_m_s": 2.4,
  "sent_at": 1761238800000
}
```
La Lambda descarta un evento si el JSON está roto, si falta un campo, si un campo tiene un tipo inválido o si un valor está fuera de rango. Si el JSON no se puede leer, el descarte se guarda con el id del mensaje de SQS (`sqs-...`).

### Llave de DynamoDB
Clave de partición **`event_id`** (cadena, UUID):
- Cada evento tiene su propia fila, sin choques.
- SQS puede entregar un mensaje más de una vez; con la misma llave, la segunda escritura reemplaza a la primera en vez de duplicarla.
- Al ser valores aleatorios, las escrituras se reparten de forma pareja.

### Resultados
| | Eventos |
|---|---|
| Publicados en IoT Core | 10.000 |
| Guardados en DynamoDB | 9.521 |
| Descartados | 479 |
| Guardados + descartados | 10.000 ✔ |

| Motivo de descarte | Eventos |
|---|---|
| Falta campo | 208 |
| Fuera de rango | 114 |
| Tipo inválido | 106 |
| JSON inválido | 51 |

**Retraso desde el envío hasta DynamoDB:** promedio 543 ms, máximo 2.306 ms, mínimo 27 ms.

### Cómo se ejecuta
En AWS CloudShell (us-east-2):
```bash
python3 publicar_eventos.py      # publica 10.000 eventos
python3 contar_resultados.py     # cuadre y retraso
```

---

## Preguntas finales

**Parte A – Si los datos fueran 1.000 veces mayores (~1.300 millones de filas, ~80 GB):**
1. Guardar bronce comprimido (Parquet) o partido en varios archivos, no un solo CSV.
2. Particionar las tablas Iceberg por año, mes y municipio.
3. Cargar por lotes con `INSERT INTO` / `MERGE`, y usar Spark (Glue o EMR) para el procesamiento pesado.
4. Hacer mantenimiento de Iceberg (`OPTIMIZE`, `VACUUM`).
5. Automatizar la descarga en paralelo y poner límites de costo en el grupo de trabajo de Athena.

**Parte B – ¿A partir de qué volumen dejaría este flujo?**
A partir de unos miles de eventos por segundo sostenidos, o antes si se necesita orden, volver a procesar eventos o varios consumidores. Usaría **Kinesis Data Streams**: mantiene el orden por partición, guarda los eventos para volver a leerlos, admite varios consumidores y procesa en lotes grandes, lo que baja el costo por evento. Para volúmenes mucho mayores, o si ya se usa Kafka, **Amazon MSK**.

---

## Capturas
En la carpeta `capturas/` (el número de cuenta de AWS está tapado):

**Parte A**
- `A0_descripcion_csv.png` – filas, columnas, peso y primeras filas del CSV
- `A0b_tabla_bronce_columnas.png` – tabla de bronce con todas las columnas como texto
- `A1_hallazgos_datos_originales.png` – hallazgos con los datos originales (todo en 0)
- `A2_hallazgos_con_errores.png` – hallazgos con los errores simulados
- `A3_rechazadas_por_motivo.png` – filas rechazadas por regla
- `A4_cuadre_plata.png` – bronce = válidas + rechazadas
- `A5_oro_tabla.png` – tabla de oro
- `A6_oro_ranking.png` – ranking de municipios por potencial solar

**Parte B**
- `B1_regla_iot_lista.png`, `B2_regla_iot_sql.png`, `B3_regla_iot_accion_sqs.png` – regla de IoT Core
- `B4_cola_sqs_detalles.png` – cola SQS con la Lambda conectada
- `B5_lambda_diagrama_sqs.png`, `B6_lambda_codigo.png` – función Lambda
- `B7_dynamodb_tablas_llave.png` – tablas de DynamoDB con la llave `event_id`
- `B8_recuento_guardados_9521.png`, `B9_recuento_descartados_479.png` – recuento en DynamoDB
- `B10_descartados_motivo.png` – eventos descartados con su motivo
- `B11_publicacion_10000_eventos.png` – publicación de los 10.000 eventos
- `B12_cuadre_y_retraso.png` – cuadre de eventos y retraso
