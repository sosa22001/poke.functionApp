import azure.functions as func
import datetime
import io
import os
import json
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")    

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = func.FunctionApp()


@app.queue_trigger(arg_name="azqueue", queue_name="requests",
                               connection="QueueAzureWebJobsStorage") 
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    logger.info(f"Queue trigger function processed message: {record}")
    id = record[0]["id"]

    update_request(id, "inprogress")

    logging.info(f"Queue trigger function processed message: {id}")

    #Aquí obtenemos el pokemon o al menos su detalle
    request = get_request(id)
    pokemons = get_pokemons(request["type"])

    sample_size = request.get("SampleSize")
    logger.info(f"Sample size: {sample_size}")

    if (
        sample_size
        and isinstance(sample_size, int)
        and sample_size > 0
        and sample_size < len(pokemons)
    ):
        logger.info(f"Se aplicará un muestreo de {sample_size} registros.")
        import random
        pokemons = random.sample(pokemons, sample_size)
        logger.info(f"Muestreo aplicado: {sample_size} registros seleccionados.")
        logger.info(f"Total de Pokémon en CSV: {len(pokemons)}")
    else:
        logger.info(f"Se usará la lista completa: {len(pokemons)} Pokémon")

    pokemons_byte = generate_csv_to_blob(pokemons)
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob(blob_name= blob_name, csv_data=pokemons_byte)
    logger.info(f"Blob {blob_name} uploaded successfully")

    url_completa = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
    update_request(id, "completed", url_completa)

def update_request(id: int, status: str, url:str = None):
    payload = {
        "status": status,
        "id": id
    }

    if url:
        payload["url"] = url

    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    return response.json()

def get_request(id: int) -> dict:
    response = requests.get(f"{DOMAIN}/api/request/{id}")
    return response.json()[0]

def get_pokemons(type:str) -> list:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=3000)

    data = response.json()
    pokemon_enrties = data.get("pokemon", [])
    
    return [p["pokemon"] for p in pokemon_enrties]

def generate_csv_to_blob(pokemon_entries: list) -> bytes:
    """
    Recibe la lista completa de entries (cada uno con 'pokemon': { name, url })
    """
    enriched_data = []

    for entry in pokemon_entries:
        name = entry["name"]
        url = entry["url"]


        logging.info(f"Obteniendo detalles para: {name}")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Extraer estadísticas
            stats = {stat["stat"]["name"]: stat["base_stat"] for stat in data.get("stats", [])}

            # Extraer habilidades
            abilities = [a["ability"]["name"] for a in data.get("abilities", [])]
            abilities_str = ", ".join(abilities)

            enriched_data.append({
                "Name": name,
                "URL": url,
                "HP": stats.get("hp", 0),
                "Attack": stats.get("attack", 0),
                "Defense": stats.get("defense", 0),
                "Special Attack": stats.get("special-attack", 0),
                "Special Defense": stats.get("special-defense", 0),
                "Speed": stats.get("speed", 0),
                "Abilities": abilities_str
            })

        except Exception as e:
            logging.warning(f"No se pudo obtener detalles de {name}: {e}")
            enriched_data.append({
                "Name": name,
                "URL": url,
                "HP": None,
                "Attack": None,
                "Defense": None,
                "Special Attack": None,
                "Special Defense": None,
                "Speed": None,
                "Abilities": "N/A"
            })

    # Convertir a CSV
    df = pd.DataFrame(enriched_data)
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes


#El blob name es el nombre que queremos que tenga en el blob storage
def upload_csv_to_blob( blob_name: str, csv_data: bytes ):
    try:
        blob_service_client = BlobServiceClient.from_connection_string( AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client( container = BLOB_CONTAINER_NAME, blob=blob_name )
        blob_client.upload_blob( csv_data , overwrite=True )
    except Exception as e:
        logger.error(f"Error al subir el archivo {e} ")
        raise