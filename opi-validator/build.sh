#!/usr/bin/env sh
# Questo script shell deve essere presente in tutti i progetti Python da rilasciare:
# la pipeline Jenkins considera progetti validi solo quelli che contengono questo file.
#
# È possibile utilizzare qualunque combinazione di tool di packaging: setuptools,
# distutils, pybuilder, poetry... purché al termine della procedura i pacchetti da
# rilasciare su PyPI si trovino nella cartella $PROJECT_TWINE_DIR (variabile impostata da Jenkins).
# Opzionalmente, è possibile rilasciare nella cartella $PROJECT_COVERAGE_DIR i report
# di coverage in formato XML.
#
# Lo script viene lanciato con un ambiente virtuale di build attivo, quindi qualsiasi
# operazione effettuata sull'ambiente python (pip ecc) rimane confinata all'interno di questo.

install_tool() {
  # Installazione dei tool per gestire il processo di build
  pip install -r requirements_build.txt
}

build() {
  # Creazione pacchetti distribuibili (sdist e wheel)
  # ad esempio:
  #    poetry build
  #
  # È possibile utilizzare la variabile $ENVIRONMENT per accedere
  # al profilo richiesto dalla pipeline.
  export POETRY_CACHE_DIR=".pypoetry"
  
  # Build standard del pacchetto wheel e sdist
  poetry build
  
  # Generazione del pacchetto .dist.tar.gz per il deploy su RHEL
  poetry pack-dist --platform "$PLATFORM"
}

release() {
  # Rilascio degli artefatti su cartella $PROJECT_TWINE_DIR (OBBLIGATORIO) e dei report
  # di coverage sulla cartella $PROJECT_COVERAGE_DIR
  
  # Copia dei file generati nella cartella che Jenkins userà per il rilascio
  cp dist/*.whl "$PROJECT_TWINE_DIR"/
  cp dist/*.tar.gz "$PROJECT_TWINE_DIR"/
}