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
    id = record[0]["id"]

    update_request(id, "inprogress")

    logging.info(f"Queue trigger function processed message: {id}")

    #Aquí obtenemos el pokemon o al menos su detalle
    request = get_request(id)
    pokemons = get_pokemons(request["type"])
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

def generate_csv_to_blob( pokemon_list: list ) -> bytes:
    df = pd.DataFrame( pokemon_list )
    output = io.StringIO()
    df.to_csv( output , index=False, encoding='utf-8' )
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