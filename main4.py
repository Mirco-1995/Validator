import os
import shutil
import logging
import zipfile
import tempfile
import argparse
from pathlib import Path
from minio import Minio
from dotenv import load_dotenv
from lxml import etree

logging.basicConfig(
    filename='log.txt',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

def compila_schema_xsd(xsd_file):
    """Compila lo schema XSD una sola volta."""
    try:
        with open(xsd_file, 'rb') as schema_file:
            schema_root = etree.XML(schema_file.read())
            return etree.XMLSchema(schema_root)
    except Exception as e:
        logging.error(f"Errore nella compilazione dello schema XSD {xsd_file}: {e}")
        return None


def valida_xml(xml_file, schema):
    """Valida un file XML utilizzando lo schema XSD precompilato."""
    if schema is None:
        return False
    try:
        parser = etree.XMLParser(schema=schema)
        with open(xml_file, 'rb') as f:
            etree.fromstring(f.read(), parser)
        return True
    except (etree.XMLSyntaxError, etree.DocumentInvalid) as e:
        logging.error(f"Errore nella validazione di {xml_file}: {e}")
        return False


def processa_file_xml(xml_file, schema, directory_ok, directory_ko):
    """Valida un file XML e lo sposta nella cartella appropriata."""
    if valida_xml(xml_file, schema):
        shutil.move(xml_file, os.path.join(directory_ok, os.path.basename(xml_file)))
        logging.info(f"{os.path.basename(xml_file)} è valido. Spostato in {directory_ok}.")
        print(f"{os.path.basename(xml_file)} è valido.")
    else:
        shutil.move(xml_file, os.path.join(directory_ko, os.path.basename(xml_file)))
        logging.info(f"{os.path.basename(xml_file)} non è valido. Spostato in {directory_ko}.")
        print(f"{os.path.basename(xml_file)} non è valido.")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    args = parser.parse_args()
    
    load_dotenv()
    
    return args.file

def processa_xml(directory_xml, xsd_file, directory_ok, directory_ko):
    """Processa tutti i file XML nella cartella senza multiprocessing."""
    if not os.path.exists(directory_ok):
        os.makedirs(directory_ok)
    if not os.path.exists(directory_ko):
        os.makedirs(directory_ko)

    # Compila lo schema XSD una volta sola
    schema = compila_schema_xsd(xsd_file)

    if schema is None:
        logging.error("Compilazione dello schema XSD fallita. Il programma si interrompe.")
        return

    # Ottieni la lista dei file XML da validare
    files_xml = [os.path.join(directory_xml, file_name) for file_name in os.listdir(directory_xml) if file_name.endswith('.xml')]

    # Processa i file uno alla volta
    for xml_file in files_xml:
        processa_file_xml(xml_file, schema, directory_ok, directory_ko)


def get_s3_config():
    s3_host = os.getenv("S3_HOST")
    s3_port = os.getenv("S3_PORT")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")
    s3_bucket = os.getenv("S3_BUCKET")

    endpoint = f"{s3_host}:{s3_port}"
    minio_client = Minio(
        endpoint,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        secure=False,
    )

    return minio_client, s3_bucket


zip_key = get_args()

minio_client, s3_bucket = get_s3_config()

with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_path = Path(tmp_dir)

    zip_local_path = tmp_path/"archive.zip"

    minio_client.fget_object(s3_bucket, zip_key, str(zip_local_path))

    logging.info(f"Estrazione archivio ZIP: {zip_local_path}...")
    with zipfile.ZipFile(zip_local_path, "r") as zf:
        zf.extractall(tmp_path)

    directory_xml = str(tmp_path)

    # Impostazioni delle directory e del file XSD
    xsd_file = os.getenv("XSD_FILE")
    directory_ok = os.getenv("XML_OK_DIRECTORY")
    directory_ko = os.getenv("XML_KO_DIRECTORY")

    # Esegui il programma senza multiprocessing
    processa_xml(directory_xml, xsd_file, directory_ok, directory_ko)