# Acta de la sesión 19

## 1. Job batch

`spark/job_01.py` lee el Parquet limpio y calcula la cobertura por indicador
y década mediante RDD, DataFrame y Spark SQL. También contrasta el resultado
con SQLite. `job_01` termina porque trabaja con un conjunto de datos acotado:
escribe el resultado y cierra la sesión Spark.

## 2. Query continuo

`temp-stream` consume continuamente el topic Kafka `sensores.temp`, interpreta
el JSON, aplica un watermark de diez minutos y cuenta eventos por sensor en
ventanas de un minuto. No termina por sí solo porque espera nuevos eventos.

## 3. Memoria

El checkpoint conserva el progreso de offsets de Kafka, la configuración del
query y el estado necesario para las ventanas y agregaciones. Gracias a ello,
un reinicio puede continuar desde el progreso confirmado sin reprocesar todo.

## 4. Parar y arrancar

El programa usa una ruta estable y detiene los queries limpiamente al recibir
Ctrl+C, sin borrar el checkpoint. La prueba real de recuperación en DataLab
queda pendiente; no se afirma un resultado que no se haya observado.

## 5. Eventos con esquema roto

El segundo query `temp-nulos` selecciona eventos cuyo campo `valor` es nulo.
Necesita su propio checkpoint porque cada query mantiene progreso y estado
independientes. Compartir la ruta con `temp-stream` mezclaría metadatos de dos
planes distintos y haría insegura la recuperación.
