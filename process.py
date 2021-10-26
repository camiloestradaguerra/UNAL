import sys
from class_sepp_model12 import *
from sepp_auxiliar_functions11 import *
import logging
import argparse
import constants_manager as c
import geopandas as gpd
import os
from utilis import *

sepp_mod = ModeloRinhas()

fecha_inicial = "2021-01-01 00:00:00"
fecha_final = "2021-01-02 23:59:59"

#fecha_inicial_pr = "2021-02-01 00:00:00"
#fecha_final_pr = "2021-02-01 04:00:00"

def process(log_file, sub_process):
    logging.basicConfig(filename=log_file,level=logging.DEBUG)
    logging.debug('Empezó función process.')

    try:
        sepp_model = ModeloRinhas()
        
        if sub_process == "clean":
            sepp_model.preprocdatos_model(fecha_inicial, fecha_final)
        elif subprocess == "train":
            if os.path.exists('./eventos_covariados.geojson') == False:
                print("Primero se debe hacer el proceso de Preprocesamiento de Datos.")
            else:
                datos_eventos = gpd.read_file('eventos_covariados.geojson')
                datos_eventos = sepp_model.preprocdatos_model(fecha_inicial, fecha_final)
        
    except Exception as e:
        msg_error = "No se completó función process"
        logging.error(msg_error)
        raise Exception(msg_error + " / " +str(e))                 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Función ejecución proceso')
    parser.add_argument("--log_file", required=True, help="Dirección archivo de log")
    parser.add_argument("--subprocess", required=True, help="Subproceso a ejecutar", default="clean", choices=["clean", "train", "predict"])
    
    args = parser.parse_args()
    subprocess=args.subprocess
    log_file = args.log_file

    try:
        process(log_file, subprocess)
    
    except Exception as e:
        raise Exception(e)