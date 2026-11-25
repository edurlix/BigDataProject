# Observatorio país — framework mínimo (Práctica 1 Big Data)

ETL de universo completo alineado con el enunciado:
**`raw → clean/Parquet → serving/SQLite → Spark`**.

## Arranque

```bash
cd practica_01_BigData_starter
py -3.11 -m venv C:\venvs\observatorio
C:\venvs\observatorio\Scripts\activate
pip install -r requirements.txt
python -m src.pipeline
python spark/job_01.py
```

En Windows se recomienda una ruta corta para el entorno virtual porque el
paquete PySpark contiene nombres largos.

Salidas:

| Zona | Ruta |
| --- | --- |
| bruto | `data/raw/*.json` + `data/raw/manifest.csv` |
| limpio | CSV de auditoría + `countries.parquet`, `languages.parquet` y dataset Hive `indicators/` |
| servicio | `data/serving/observatorio.sqlite` |
| Spark | `data/serving/spark/cobertura_decada/` y `logs/spark-events/` |
| fichas | `cards/ESP.md`, … |

Reutilizar descargas ya hechas:

```bash
python -m src.pipeline --skip-extract
```

Forzar una actualización de raw: `python -m src.pipeline --force`.

Pasos sueltos: `python -m src.extract` · `python -m src.transform` ·
`python -m src.load` · `python -m src.build_cards`.

## Fuentes y universo

`sources.yaml` obtiene el perfil completo, seis series históricas WDI,
Eurostat y OWID. Las equivalencias externas se declaran en `code_aliases`.
El fichero `excluded_entities.csv` explica qué entidades no cumplen el
criterio de país.

## Cómo cambiar o añadir URLs

1. Editad un bloque en `sources.yaml` (fija o con `{iso2}` / `{iso3}`).
2. Si el JSON/CSV no encaja, cread una función en `src/transform.py` y
   registradla en `TRANSFORMERS`.
3. Poned `transformer: nombre_de_vuestra_funcion` en el YAML.
4. Ejecutad de nuevo `python -m src.pipeline`.

Transformers incluidos: `countries_profile`, `worldbank_indicator`,
`eurostat_demo_pjan`, `owid_life_expectancy`, `generic_passthrough`.

## Reglas del framework (no romper)

- **Extract** no limpia ni une fuentes.
- **Transform** no descarga; solo lee `data/raw/`.
- **Load / fichas** no leen JSON bruto; solo `clean/` → `serving/`.
- Los países de trabajo están en `focus_countries` del YAML (mapeo ISO2↔ISO3).
- Transform obligatorio: `iso3` canónico, idiomas en tabla larga, esquema
  común de indicadores, `join_orphans.csv`.

## Informe

Completad el informe en `informe/` y los insights del enunciado.
No subáis `.venv/` ni cachés.
