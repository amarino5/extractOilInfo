#Libreria que extrae los precios de las gasolineras y los vuelca en un mongo
#===========================================================================

import json
import urllib2
import logging
from pymongo import MongoClient

logging.basicConfig(filename='/home/osboxes/dev/extractOilInfo/log/execution.log',level=logging.DEBUG)
logging.info("Starting getting oil information.....")
url_precios = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
try:
    precios = json.load(urllib2.urlopen(url_precios))
    collecion_precios = precios["ListaEESSPrecio"]
    logging.debug("lista de gasolineras : %s",collecion_precios)
except Exception as e:
    logging.error("Error getting information from source: %s", e.message)

try:
    with open("/home/osboxes/dev/extractOilInfo/temp.json", "w") as outfile:
        json.dump(precios, outfile, indent=4, separators=(',', ':'), encoding='utf-8')
except Exception as e:
    logging.error("Unable to create json temp file : %s", e.message)

try:
    mongoClient = MongoClient()
    db = mongoClient.nextSeguros
    collection = db.oilInfo
    result = collection.insert_many(collecion_precios)
    logging.debug("IDs insertados:",result)
except Exception as e:
    logging.error("Unable to insert Oil info collection: %s", e.message)