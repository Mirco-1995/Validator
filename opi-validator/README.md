# OPI Validator
Validatore di flussi XML per l'applicativo OPI.

## Funzionamento
Il software preleva archivi ZIP da un server MinIO, estrae i file XML contenuti e li valida utilizzando lo schema XSD ufficiale (`Tesoreria_Disposizioni.xsd`).
I file validi vengono spostati nella cartella di successo, quelli non validi nella cartella di errore.

## Requisiti
- Python >= 3.9
- Poetry