# Entrega 02 — universo y Spark

## Predicción

Se esperaba que RDD, DataFrame, Spark SQL y SQLite produjeran exactamente
la misma cobertura por indicador y década al excluir valores nulos.
También se esperaba un *shuffle* en las agregaciones por clave.

## Resultado

El programa `spark/job_01.py` implementa las cuatro versiones y termina con
código distinto de cero si sus resultados difieren. La ejecución comprobada
en el entorno de entrega debe anotarse aquí con el número de grupos obtenido:

- Grupos `(indicator_id, década)`: **pendiente de regenerar los datos reales**.
- Particiones de entrada: **pendiente de la ejecución final**.
- Resultado de la comparación: **pendiente de la ejecución final**.

No se incluyen cifras del mini-dataset de prototipos porque no serían evidencia
de este observatorio.

## Decisiones

- El universo sale del perfil completo y no de una lista fija de países.
- `indicator_id` es la partición Hive porque se consulta repetidamente y tiene
  cardinalidad mucho menor que país o año.
- Los nulos no cuentan como observaciones y nunca se sustituyen por cero.
- El SQLite se regenera desde `clean` y actúa como cuarta implementación de
  contraste independiente de Spark.

## Evidencia

Spark escribe el event log en `logs/spark-events/`. Tras una ejecución final
correcta se debe copiar su subdirectorio completo a `evidencias/spark-events/`
y sustituir el README provisional de esa carpeta. `data/` y `logs/` permanecen
fuera de Git porque son regenerables.

## Preguntas de teoría

### 1. ¿Para qué sirve el event log?

El event log permite reconstruir la ejecución en Spark History Server:
trabajos, etapas, tareas, particiones, tiempos y volumen de *shuffle*. A
diferencia de una captura, conserva evidencia inspeccionable del trabajo real.

### 2. ¿Dónde aparecen las particiones y el shuffle?

El job imprime el número de particiones del DataFrame leído mediante
`getNumPartitions()`. En el plan físico, `groupBy`, `reduceByKey` y
`COUNT(DISTINCT país)` requieren redistribuir registros por
`(indicator_id, década)`, por lo que aparece un operador `Exchange`, que es el
*shuffle*. La cifra concreta queda pendiente de la ejecución con los datos
regenerados.

### 3. ¿CSV o Parquet ocupa menos?

Parquet suele ocupar menos por su representación columnar, tipos binarios y
compresión; además permite leer solo columnas y podar particiones. La comparación
de bytes entre el CSV real y el dataset Parquet queda pendiente de regenerar
ambas salidas en la máquina de entrega, sin reutilizar cifras de ejemplo.

## Pendientes de cierre

1. Ejecutar el pipeline completo con acceso a las APIs.
2. Ejecutar `python spark/job_01.py` y completar las cifras anteriores.
3. Copiar el event log real a `evidencias/`.
4. Revisar los artefactos y crear la etiqueta `entrega-02`.
