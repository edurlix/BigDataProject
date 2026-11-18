"""Transform: data/raw/ → data/clean/ (tablas normalizadas).

Obligatorio en la Práctica 1 Big Data (ver enunciado):
  - clave canónica iso3 (+ mapeo iso2↔iso3 vía focus_countries)
  - perfil aplanado → countries
  - explosión de idiomas → languages (tabla larga)
  - indicadores en esquema común → indicators
  - tipado / nulos; join por iso3; join_orphans.csv

Para una URL nueva: implementad una función y registradla en TRANSFORMERS
con el mismo nombre que `transformer:` en sources.yaml.
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil
from typing import Callable

import pandas as pd

from .config import Config, Source, load_config
from .paths import CLEAN, RAW, ensure_dirs

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]
INDICATOR_COLUMNS = [
    "iso3", "indicator_id", "indicator_label", "year",
    "value", "unit", "source_id",
]


def _read_json(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.json"
    payload = _read_json(raw_path)
    wanted = {c.iso3 for c in cfg.focus_countries}

    countries_rows = []
    language_rows = []
    excluded_rows = []
    for item in payload:
        iso3 = item.get("cca3")
        accepted_country = bool(
            isinstance(iso3, str)
            and len(iso3) == 3
            and (item.get("name") or {}).get("common")
            and item.get("region")
            and item.get("independent", True) is not False
        )
        if cfg.universe and not accepted_country:
            excluded_rows.append(
                {
                    "source_id": source.id,
                    "source_code": iso3 or item.get("cca2"),
                    "name": (item.get("name") or {}).get("common"),
                    "reason": "No cumple el criterio de país independiente con ISO3 y región",
                }
            )
            continue
        if not cfg.universe and wanted and iso3 not in wanted:
            continue
        name = item.get("name") or {}
        countries_rows.append(
            {
                "iso3": iso3,
                "iso2": item.get("cca2"),
                "name_common": name.get("common"),
                "name_official": name.get("official"),
                "region": item.get("region"),
                "subregion": item.get("subregion"),
            }
        )
        for code, lang_name in (item.get("languages") or {}).items():
            language_rows.append(
                {
                    "iso3": iso3,
                    "language_code": code,
                    "language_name": lang_name,
                }
            )

    return {
        "countries": pd.DataFrame(countries_rows),
        "languages": pd.DataFrame(language_rows),
        "excluded_entities": pd.DataFrame(
            excluded_rows,
            columns=["source_id", "source_code", "name", "reason"],
        ),
    }


def _profile_crosswalk(cfg: Config) -> tuple[dict[str, str], dict[str, str]]:
    """Equivalencias declarativas ISO2/alias → ISO3 para todas las fuentes."""
    iso2_to_iso3 = {c.iso2: c.iso3 for c in cfg.focus_countries}
    profile = RAW / "countries_profile.json"
    if profile.exists():
        for item in _read_json(profile):
            if item.get("cca2") and item.get("cca3"):
                iso2_to_iso3[str(item["cca2"])] = str(item["cca3"])
    aliases = {**iso2_to_iso3, **cfg.code_aliases}
    iso3_to_iso2 = {iso3: iso2 for iso2, iso3 in iso2_to_iso3.items()}
    return aliases, iso3_to_iso2


def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    rows = []
    meta = source.meta or {}
    aliases, iso3_to_iso2 = _profile_crosswalk(cfg)
    fixed_path = RAW / f"{source.id}.json"
    inputs: list[tuple[Path, str | None, str | None]] = []
    if fixed_path.exists():
        inputs.append((fixed_path, None, None))
    else:
        inputs.extend(
            (RAW / f"{source.id}_{country.iso2}.json", country.iso2, country.iso3)
            for country in cfg.focus_countries
        )

    for path, fallback_iso2, fallback_iso3 in inputs:
        if not path.exists():
            continue
        payload = _read_json(path)
        # World Bank: [meta, [records...]]
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            continue
        for rec in payload[1]:
            if rec is None:
                continue
            iso3 = rec.get("countryiso3code") or fallback_iso3
            iso2 = (rec.get("country") or {}).get("id") or fallback_iso2
            iso3 = cfg.code_aliases.get(str(iso3), iso3) if iso3 else aliases.get(str(iso2))
            if not iso3:
                continue
            rows.append(
                {
                    "iso3": iso3,
                    "iso2": iso2 or iso3_to_iso2.get(str(iso3)),
                    "indicator_id": meta.get("indicator_id", source.id),
                    "indicator_label": meta.get(
                        "indicator_label",
                        (rec.get("indicator") or {}).get("value"),
                    ),
                    "year": int(rec["date"]) if str(rec.get("date", "")).isdigit() else None,
                    "value": rec.get("value"),
                    "unit": meta.get("unit"),
                    "source_id": source.id,
                }
            )
    return {"indicators": pd.DataFrame(rows, columns=INDICATOR_COLUMNS + ["iso2"])}


def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.csv"
    df = pd.read_csv(raw_path)
    meta = source.meta or {}
    requested_year = str(meta["year"]) if "year" in meta else None
    wanted = {c.iso3 for c in cfg.focus_countries}
    # Columnas OWID: Entity, Code, Year, Life expectancy
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
    _aliases, iso2_by_iso3 = _profile_crosswalk(cfg)
    sub = df[df[code_col].notna()]
    if not cfg.universe and wanted:
        sub = sub[sub[code_col].isin(wanted)]
    if requested_year is not None:  # compatibilidad con configuraciones antiguas
        sub = sub[sub[year_col].astype(str) == requested_year]
    rows = []
    for _, r in sub.iterrows():
        iso3 = r[code_col]
        rows.append(
            {
                "iso3": iso3,
                "iso2": iso2_by_iso3.get(iso3),
                "indicator_id": meta.get("indicator_id", "life_expectancy"),
                "indicator_label": meta.get("indicator_label", value_col),
                "year": int(r[year_col]),
                "value": r[value_col],
                "unit": meta.get("unit", "years"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows, columns=INDICATOR_COLUMNS + ["iso2"])}


def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    rows = []
    meta = source.meta or {}
    fixed_path = RAW / f"{source.id}.json"
    if fixed_path.exists():
        payload = _read_json(fixed_path)
        aliases, iso3_to_iso2 = _profile_crosswalk(cfg)
        dimensions = payload.get("id") or []
        sizes = payload.get("size") or []
        dimension_data = payload.get("dimension") or {}
        category_codes: dict[str, dict[int, str]] = {}
        for dim in dimensions:
            index = (
                ((dimension_data.get(dim) or {}).get("category") or {}).get("index")
                or {}
            )
            if isinstance(index, dict):
                category_codes[dim] = {int(pos): str(code) for code, pos in index.items()}
            else:
                category_codes[dim] = {pos: str(code) for pos, code in enumerate(index)}

        for flat_index, value in (payload.get("value") or {}).items():
            remainder = int(flat_index)
            positions = [0] * len(sizes)
            for i in range(len(sizes) - 1, -1, -1):
                positions[i] = remainder % int(sizes[i])
                remainder //= int(sizes[i])
            coords = {
                dim: category_codes.get(dim, {}).get(positions[i])
                for i, dim in enumerate(dimensions)
            }
            geo = coords.get("geo")
            iso3 = aliases.get(str(geo), cfg.code_aliases.get(str(geo), geo))
            year = coords.get("time")
            if not iso3 or not str(year).isdigit():
                continue
            rows.append(
                {
                    "iso3": iso3,
                    "iso2": iso3_to_iso2.get(str(iso3), geo),
                    "indicator_id": meta.get("indicator_id", "demo_pjan"),
                    "indicator_label": meta.get("indicator_label", payload.get("label")),
                    "year": int(year),
                    "value": value,
                    "unit": meta.get("unit", "persons"),
                    "source_id": source.id,
                }
            )
        return {"indicators": pd.DataFrame(rows, columns=INDICATOR_COLUMNS + ["iso2"])}

    for country in cfg.focus_countries:
        path = RAW / f"{source.id}_{country.iso2}.json"
        if not path.exists():
            continue
        payload = _read_json(path)
        values = payload.get("value") or {}
        if not values:
            # País fuera de cobertura Eurostat: huérfano documentado vía ausencia
            continue
        # Con una sola celda filtrada, la clave suele ser "0"
        value = next(iter(values.values()))
        rows.append(
            {
                "iso3": country.iso3,
                "iso2": country.iso2,
                "indicator_id": meta.get("indicator_id", "demo_pjan"),
                "indicator_label": meta.get(
                    "indicator_label", payload.get("label")
                ),
                "year": int(meta["time"]) if str(meta.get("time", "")).isdigit() else None,
                "value": value,
                "unit": meta.get("unit", "persons"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows, columns=INDICATOR_COLUMNS + ["iso2"])}


def transform_generic_passthrough(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    """Plantilla: lee raw y deja un DataFrame mínimo para que lo adapteis."""
    paths = sorted(RAW.glob(f"{source.id}*"))
    rows = []
    for path in paths:
        rows.append(
            {
                "source_id": source.id,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "note": "Reemplazad este transformer por uno específico",
            }
        )
    return {"custom_raw_index": pd.DataFrame(rows)}


TRANSFORMERS: dict[str, Transformer] = {
    "countries_profile": transform_countries_profile,
    "worldbank_indicator": transform_worldbank_indicator,
    "eurostat_demo_pjan": transform_eurostat_demo_pjan,
    "owid_life_expectancy": transform_owid_life_expectancy,
    "generic_passthrough": transform_generic_passthrough,
}


def run_transform(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    ensure_dirs()
    buckets: dict[str, list[pd.DataFrame]] = {}

    for source in cfg.sources:
        name = source.transformer
        if name not in TRANSFORMERS:
            raise KeyError(
                f"Transformer '{name}' no registrado. "
                f"Disponibles: {sorted(TRANSFORMERS)}"
            )
        print(f"[transform] {source.id} → {name}")
        out = TRANSFORMERS[name](source, cfg)
        for table, df in out.items():
            buckets.setdefault(table, []).append(df)

    merged: dict[str, pd.DataFrame] = {}
    for table, parts in buckets.items():
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if table == "indicators":
            for col in INDICATOR_COLUMNS:
                if col not in df:
                    df[col] = pd.NA
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            unique_key = ["iso3", "indicator_id", "year", "source_id"]
            duplicated = df.duplicated(unique_key, keep=False)
            if duplicated.any():
                raise ValueError(
                    "Clave única duplicada en indicators "
                    "(iso3, indicator_id, year, source_id)"
                )
        dest = CLEAN / f"{table}.csv"
        df.to_csv(dest, index=False)
        print(f"[transform] {table}: {len(df)} filas → {dest}")
        merged[table] = df

    # Cobertura del cruce: huérfanos explícitos por fuente e indicador.
    if "countries" in merged and "indicators" in merged:
        profile = sorted(set(merged["countries"]["iso3"].dropna().unique()))
        orphan_rows = []
        indicator_sources = [
            (source.id, (source.meta or {}).get("indicator_id", source.id))
            for source in cfg.sources
            if source.transformer
            in {"worldbank_indicator", "eurostat_demo_pjan", "owid_life_expectancy"}
        ]
        for source_id, indicator_id in indicator_sources:
            subset = merged["indicators"][
                (merged["indicators"]["source_id"] == source_id)
                & (merged["indicators"]["indicator_id"] == indicator_id)
                & merged["indicators"]["value"].notna()
            ]
            have = set(subset["iso3"].dropna().unique())
            for iso3 in sorted(set(profile) - have):
                orphan_rows.append(
                    {
                        "iso3": iso3,
                        "source_id": source_id,
                        "indicator_id": indicator_id,
                        "reason": "sin observaciones no nulas",
                    }
                )
        report = pd.DataFrame(
            orphan_rows,
            columns=["iso3", "source_id", "indicator_id", "reason"],
        )
        report_path = CLEAN / "join_orphans.csv"
        report.to_csv(report_path, index=False)
        merged["join_orphans"] = report
        print(f"[transform] huérfanos del join: {len(report)} → {report_path}")

    # Contrato de clean: Parquet tipado siempre, con indicators particionado Hive.
    parquet_files = {
        "countries": CLEAN / "countries.parquet",
        "languages": CLEAN / "languages.parquet",
        "excluded_entities": CLEAN / "excluded_entities.parquet",
    }
    for table, path in parquet_files.items():
        if table in merged:
            try:
                merged[table].to_parquet(path, index=False)
            except ImportError:
                print("[transform] Parquet omitido: instalad pyarrow desde requirements.txt")
                return merged

    if "indicators" in merged:
        indicators_path = CLEAN / "indicators"
        if indicators_path.exists():
            shutil.rmtree(indicators_path)
        try:
            merged["indicators"].to_parquet(
                indicators_path,
                index=False,
                partition_cols=["indicator_id"],
            )
        except ImportError:
            print("[transform] dataset Hive omitido: falta pyarrow")

    return merged


if __name__ == "__main__":
    run_transform()
