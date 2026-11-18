"""Extract: descarga intacta → data/raw/ + manifiesto.

No transforma. Añadir una URL nueva = entrada en sources.yaml.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from uuid import uuid4

import requests

from .config import Config, Source, load_config
from .paths import MANIFEST, RAW, ensure_dirs

USER_AGENT = "UCJC-DSBD-observatorio-paises/Eddyson-Urena"
TIMEOUT = 60
MAX_REQUESTS_PER_SECOND = 5
MAX_RETRIES = 3
RETRY_STATUS = {429, 500, 502, 503, 504}  # 404 es definitivo
RATE_INTERVAL = 1 / MAX_REQUESTS_PER_SECOND


def _raw_name(source: Source, iso2: str | None = None) -> str:
    if source.url_template and iso2:
        return f"{source.id}_{iso2}.{source.format}"
    return f"{source.id}.{source.format}"


def _request(url: str) -> tuple[int, bytes | None, int, str]:
    """GET cortés con backoff; solo devuelve contenido para HTTP 200."""
    last_note = ""
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            response = requests.get(
                url,
                timeout=TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            status = int(response.status_code)
            if status == 200:
                return status, response.content, attempt, ""
            last_note = f"HTTP {status}"
            if status not in RETRY_STATUS:  # incluye 404: no se reintenta
                return status, None, attempt, last_note
        except requests.RequestException as exc:
            status = 0
            last_note = str(exc)

        if attempt <= MAX_RETRIES:
            time.sleep(RATE_INTERVAL * (2 ** (attempt - 1)))
    return status, None, MAX_RETRIES + 1, last_note


def _download(url: str, dest: Path) -> tuple[int, int]:
    """Descarga un artefacto; una respuesta no-200 nunca se escribe en raw."""
    status, content, _attempts, _note = _request(url)
    if status == 200 and content is not None:
        dest.write_bytes(content)
        return status, len(content)
    return status, 0


def _download_detailed(url: str, dest: Path) -> tuple[int, int, int, str]:
    """Variante interna que conserva intentos y diagnóstico para el manifiesto."""
    status, content, attempts, note = _request(url)
    if status == 200 and content is not None:
        dest.write_bytes(content)
        return status, len(content), attempts, note
    return status, 0, attempts, note


def _download_worldbank(url: str, dest: Path) -> tuple[int, int, str]:
    """Recorre todas las páginas WDI y contrasta los registros con ``total``."""
    page = 1
    records: list[dict] = []
    first_meta: dict | None = None
    total = 0

    while True:
        separator = "&" if "?" in url else "?"
        page_url = f"{url}{separator}page={page}"
        status, content, _attempts, note = _request(page_url)
        if status != 200 or content is None:
            return status, 0, note
        payload = json.loads(content)
        if not isinstance(payload, list) or len(payload) < 2:
            raise ValueError(f"Respuesta World Bank inválida en página {page}")
        meta = payload[0] or {}
        if first_meta is None:
            first_meta = dict(meta)
            total = int(meta.get("total", 0))
        records.extend(payload[1] or [])
        pages = int(meta.get("pages", 1))
        if page >= pages:
            break
        page += 1
        time.sleep(RATE_INTERVAL)

    if total and len(records) != total:
        raise RuntimeError(
            f"Paginación incompleta: API total={total}, descargados={len(records)}"
        )
    content = json.dumps([first_meta or {}, records], ensure_ascii=False).encode("utf-8")
    dest.write_bytes(content)
    return 200, len(content), f"{page} página(s), {len(records)} registros"


def _valid_artifact(path: Path) -> bool:
    """Criterio mínimo de idempotencia: fichero existente y no vacío."""
    return path.is_file() and path.stat().st_size > 0


def extract_source(
    source: Source,
    cfg: Config,
    *,
    force: bool = False,
    run_id: str = "",
) -> list[dict]:
    """Descarga una fuente; si tiene plantilla, una petición por país foco."""
    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    if source.url_template:
        targets = cfg.focus_countries
    else:
        targets = [None]

    for country in targets:
        iso2 = country.iso2 if country else ""
        iso3 = country.iso3 if country else ""
        url = source.resolve_url(iso2=iso2, iso3=iso3)
        filename = _raw_name(source, iso2 or None)
        dest = RAW / filename
        attempts = 0
        try:
            if _valid_artifact(dest) and not force:
                status, nbytes = 200, dest.stat().st_size
                note = "reutilizado: artefacto válido existente"
            elif source.paginate == "worldbank":
                status, nbytes, note = _download_worldbank(url, dest)
                attempts = 1
            else:
                status, nbytes, attempts, note = _download_detailed(url, dest)
        except requests.RequestException as exc:
            status, nbytes = 0, 0
            note = str(exc)
        except (ValueError, RuntimeError) as exc:
            status, nbytes = 0, 0
            note = str(exc)
        rows.append(
            {
                "run_id": run_id,
                "timestamp_utc": now,
                "source_id": source.id,
                "url": url,
                "http_status": status,
                "bytes": nbytes,
                "filename": filename,
                "iso2": iso2,
                "iso3": iso3,
                "attempts": attempts,
                "note": note,
            }
        )
    return rows


def run_extract(cfg: Config | None = None, force: bool = False) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    all_rows: list[dict] = []
    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}")
        all_rows.extend(extract_source(source, cfg, force=force, run_id=run_id))
        time.sleep(RATE_INTERVAL)

    fieldnames = [
        "run_id",
        "timestamp_utc",
        "source_id",
        "url",
        "http_status",
        "bytes",
        "filename",
        "iso2",
        "iso3",
        "attempts",
        "note",
    ]
    append = MANIFEST.exists() and MANIFEST.stat().st_size > 0
    with MANIFEST.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not append:
            writer.writeheader()
        writer.writerows(all_rows)

    print(f"[extract] manifiesto → {MANIFEST}")
    return MANIFEST


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extracción idempotente")
    parser.add_argument("--force", action="store_true", help="Repetir descargas válidas")
    run_extract(force=parser.parse_args().force)
