#Libreria que extrae los precios de las gasolineras y los vuelca en un mongo
#===========================================================================

import json
import urllib2
import logging
from pymongo import MongoClient
from cfg import oilConfig

def remove_dot_key(obj):
    for key in obj.keys():
        new_key = key.replace(".","")
        new_key = new_key.replace("$", "")
        if new_key != key:
            obj[new_key] = obj[key]
            del obj[key]
    return obj

logging.basicConfig(filename='/home/osboxes/dev/extractOilInfo/log/execution.log',level=oilConfig.log_level)
logging.info("Starting getting oil information.....")
url_prices = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
try:
    oilStations = json.load(urllib2.urlopen(url_prices),object_hook=remove_dot_key)
    logging.debug("Information retrieved")
    collection_prices = oilStations["ListaEESSPrecio"]
    logging.debug("lista de gasolineras.Numero de registros : %s",len(collection_prices))
except Exception as e:
    logging.error("Error getting information from source: %s", e.message)

try:
    with open("/home/osboxes/dev/extractOilInfo/temp.json", "w+") as outfile:
        json.dump(oilStations, outfile, indent=10, sort_keys=True, separators=(',', ':'), encoding='utf-8')
except Exception as e:
    logging.error("Unable to create json temp file : %s", e.message)

try:
    mongoClient = MongoClient(oilConfig.mongodb_ip,oilConfig.mongodb_port)
    db = mongoClient.nextSeguros
    logging.info("MongoDB connection retrieved")
    collection = db.oilInfo.drop()
    logging.info("Collection oilInfo dropped")
    for item in collection_prices:
        result = collection.insert(item)
    logging.debug("%s items inserted into oilInfo collection",len(collection_prices))
except Exception as e:
    logging.error("Unable to insert Oil info collection: %s", e.message)