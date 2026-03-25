import argparse
import os
import shutil
import zipfile
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional, List, Dict, TextIO

from minio import Minio
import oracledb
from dotenv import load_dotenv


NAMESPACE = {"opi": "http://tesoreria.bancaditalia.it/"}

log_file: Optional[TextIO] = None


def log(message: str) -> None:
    """Stampa a video e salva su file di log."""
    print(message)
    if log_file:
        log_file.write(message + "\n")
        log_file.flush()

SQL_GET_IMPORTO = """
SELECT OPI_IMPORTO_DISP_ORIGINE
FROM OPI_DISP_VARIAZIONI_ORIGINE
WHERE OPI_ID_DISPOSIZIONE_ORIGINE = :id_disposizione
  AND OPI_IBAN_ADDEBITO_ORIGINE = :iban_accredito
"""


def env(name: str) -> str:
    """Recupera una variabile d'ambiente obbligatoria."""
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Variabile d'ambiente mancante: {name}")
    return v


def parse_importo(value: str) -> Decimal:
    """Converte una stringa importo in Decimal, gestendo virgola come separatore decimale."""
    try:
        return Decimal(value.replace(",", "."))
    except InvalidOperation:
        raise ValueError(f"Importo non valido: {value}")


def extract_xml_data(xml_path: Path) -> Optional[Dict]:
    """
    Estrae dal file XML:
    - identificativoDisposizione (da chiaveDisposizioneDaVariare)
    - contoIbanAccredito
    - somma degli importoReimputazione

    Ritorna None se il file non contiene una variazioneUscita.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    variazione = root.find(".//opi:variazioneUscita", NAMESPACE)
    if variazione is None:
        return None

    chiave = variazione.find("opi:chiaveDisposizioneDaVariare", NAMESPACE)
    if chiave is None:
        return None

    id_disp_elem = chiave.find("opi:identificativoDisposizione", NAMESPACE)
    iban_elem = variazione.find("opi:contoIbanAccredito", NAMESPACE)

    if id_disp_elem is None or iban_elem is None:
        return None

    id_disposizione = id_disp_elem.text
    iban_accredito = iban_elem.text

    reimputazioni = variazione.findall("opi:reimputazione", NAMESPACE)
    somma_reimputazione = Decimal("0")

    for reimp in reimputazioni:
        importo_elem = reimp.find("opi:importoReimputazione", NAMESPACE)
        if importo_elem is not None and importo_elem.text:
            somma_reimputazione += parse_importo(importo_elem.text)

    return {
        "id_disposizione": id_disposizione,
        "iban_accredito": iban_accredito,
        "somma_reimputazione": somma_reimputazione,
    }


def get_importo_db(cursor, id_disposizione: str, iban_accredito: str) -> Optional[Decimal]:
    """Recupera l'importo dal database Oracle."""
    cursor.execute(SQL_GET_IMPORTO, {
        "id_disposizione": id_disposizione,
        "iban_accredito": iban_accredito,
    })
    row = cursor.fetchone()
    if row is None:
        return None

    importo_str = row[0]
    if importo_str is None:
        return None

    return parse_importo(str(importo_str))


def create_minio_client() -> Minio:
    """Crea il client MinIO con configurazione custom."""
    s3_host = env("S3_HOST")
    s3_port = env("S3_PORT")
    s3_access_key = env("S3_ACCESS_KEY")
    s3_secret_key = env("S3_SECRET_KEY")

    endpoint = f"{s3_host}:{s3_port}"

    return Minio(
        endpoint,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        secure=False,
    )


def download_from_s3(minio_client: Minio, s3_bucket: str, s3_key: str, dest_path: Path) -> None:
    """Scarica il file ZIP da S3/MinIO."""
    log(f"Download da s3://{s3_bucket}/{s3_key}...")
    minio_client.fget_object(s3_bucket, s3_key, str(dest_path))


def extract_zip(zip_path: Path, extract_dir: Path) -> List[Path]:
    """Estrae il file ZIP e ritorna la lista dei file XML."""
    log(f"Estrazione archivio ZIP: {zip_path}...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    xml_files = list(extract_dir.glob("**/*.xml"))
    log(f"Trovati {len(xml_files)} file XML")
    return xml_files


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Valida disposizioni XML confrontando con il database Oracle"
    )
    parser.add_argument(
        "file",
        type=str,
        help="Path al file ZIP (locale o chiave S3)"
    )
    parser.add_argument(
        "--local", "-l",
        action="store_true",
        help="Usa un file ZIP locale invece di scaricarlo da S3"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        metavar="FILE",
        help="File di output per il log (default: validate_YYYYMMDD_HHMMSS.log)"
    )
    args = parser.parse_args()

    load_dotenv()

    global log_file
    if args.output:
        log_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = f"validate_{timestamp}.log"
    log_file = open(log_path, "w", encoding="utf-8")
    log(f"Log salvato in: {log_path}")

    ora_user = env("ORA_USER")
    ora_pass = env("ORA_PASS")
    ora_dsn = env("ORA_DSN")

    errors_found = 0
    files_processed = 0
    files_skipped = 0

    with tempfile.TemporaryDirectory() as tmp_dir:
        extract_dir = Path(tmp_dir)

        if args.local:
            local_path = Path(args.file)
            if not local_path.exists():
                log(f"Errore: file non trovato: {local_path}")
                return 1
            xml_files = extract_zip(local_path, extract_dir)
        else:
            s3_bucket = env("S3_BUCKET")
            s3_key = args.file
            minio_client = create_minio_client()
            zip_path = extract_dir / "archive.zip"
            download_from_s3(minio_client, s3_bucket, s3_key, zip_path)
            xml_files = extract_zip(zip_path, extract_dir)

        if not xml_files:
            log("Nessun file XML trovato nell'archivio.")
            return 0

        with oracledb.connect(user=ora_user, password=ora_pass, dsn=ora_dsn) as conn:
            with conn.cursor() as cursor:
                for xml_path in xml_files:
                    xml_name = xml_path.name

                    try:
                        data = extract_xml_data(xml_path)
                    except ET.ParseError as e:
                        log(f"[ERRORE] {xml_name}: XML non valido - {e}")
                        errors_found += 1
                        continue
                    except ValueError as e:
                        log(f"[ERRORE] {xml_name}: {e}")
                        errors_found += 1
                        continue

                    if data is None:
                        files_skipped += 1
                        continue

                    files_processed += 1
                    id_disp = data["id_disposizione"]
                    iban = data["iban_accredito"]
                    somma_xml = data["somma_reimputazione"]

                    importo_db = get_importo_db(cursor, id_disp, iban)

                    if importo_db is None:
                        log(f"[ERRORE] {xml_name}")
                        log(f"         ID Disposizione: {id_disp}")
                        log(f"         IBAN Accredito:  {iban}")
                        log(f"         Record non trovato nel database")
                        log("")
                        errors_found += 1
                        continue

                    if importo_db != somma_xml:
                        differenza = importo_db - somma_xml
                        log(f"[ERRORE] {xml_name}")
                        log(f"         ID Disposizione:    {id_disp}")
                        log(f"         IBAN Accredito:     {iban}")
                        log(f"         Importo DB:         {importo_db}")
                        log(f"         Somma reimputazioni:{somma_xml}")
                        log(f"         Differenza:         {differenza}")
                        log("")
                        errors_found += 1

    log("-" * 60)
    log(f"File processati: {files_processed}")
    log(f"File ignorati (no variazioneUscita): {files_skipped}")
    log(f"Errori trovati: {errors_found}")

    if log_file:
        log_file.close()

    return 1 if errors_found > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())