# Defectos corregidos

## 1. Las respuestas HTTP erróneas acababan en `data/raw`

- **Reproducción:** simular una respuesta HTTP 500 y ejecutar `_download`.
- **Efecto:** el cuerpo del error se guardaba como si fuera un JSON válido y la
  transformación fallaba después con un mensaje poco relacionado.
- **Arreglo:** la descarga solo materializa contenido con HTTP 200; 404 se
  considera definitivo y 429/5xx se reintentan con espera creciente.

## 2. El pipeline volvía a descargar todos los artefactos

- **Reproducción:** ejecutar dos veces `python -m src.pipeline`.
- **Efecto:** peticiones innecesarias, más tiempo de ejecución y riesgo de
  alcanzar los límites de las APIs.
- **Arreglo:** el extract reutiliza ficheros no vacíos y `--force` es la vía
  explícita para renovarlos. El manifiesto ahora es acumulativo y lleva `run_id`.

## 3. El universo seguía limitado a cinco países

- **Reproducción:** transformar el perfil completo y contar `countries.csv`.
- **Efecto:** indicadores, SQLite y fichas omitían el resto de países.
- **Arreglo:** la configuración real activa `universe: true`, aplica un criterio
  explícito de país y documenta descartes en `excluded_entities.csv`.

## 4. La comparación de fichas mezclaba años y usaba la media

- **Reproducción:** incluir observaciones regionales de años distintos y generar
  una ficha.
- **Efecto:** el valor comparativo no representaba el mismo periodo solicitado.
- **Arreglo:** se calcula la mediana regional del mismo indicador y año, y se
  muestra claramente cuando no hay dato comparable.
