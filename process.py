import sys
from class_sepp_model import *
from sepp_auxiliar_functions import *
import logging
import argparse
import constants_manager as c
import geopandas as gpd
import os
from utilis import *

sepp_mod = ModeloRinhas()

#fecha_inicial = "2021-01-01 00:00:00"
#fecha_final = "2021-01-02 23:59:59"

#fecha_inicial_pr = "2021-02-01 00:00:00"
#fecha_final_pr = "2021-02-01 04:00:00"

def process(log_file, sub_process, fecha_inicial, fecha_final):
    logging.basicConfig(filename=log_file,level=logging.DEBUG)
    logging.debug('Empezó función process.')

    try:
        sepp_model = ModeloRinhas()
        
        if sub_process == "clean":
            sepp_model.preprocdatos_model(fecha_inicial, fecha_final)
        elif subprocess == "train":
            if os.path.exists('./eventos_covariados.geojson') == False:
                print("Primero se debe hacer el proceso de Preprocesamiento de Datos.")
                sepp_model.preprocdatos_model(fecha_inicial, fecha_final)
                datos_eventos = gpd.read_file('eventos_covariados.geojson')
                file = open("fechas_entrenamiento.txt", "w")
                file.write(str(fecha_inicial) + '\n')
                file.write(str(fecha_final) + '\n')
                file.close()
                sepp_model.train_model(datos_eventos)
            else:
                datos_eventos = gpd.read_file('eventos_covariados.geojson')
                def FECHA_mod(txt):
                    return txt.replace("T"," ")
                file = open("fechas_entrenamiento.txt", "w")
                file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[0])) + '\n')
                file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[-1])) + '\n')
                file.close()
                sepp_model.train_model(datos_eventos)
        
    except Exception as e:
        msg_error = "No se completó función process"
        logging.error(msg_error)
        raise Exception(msg_error + " / " +str(e))                 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Función ejecución proceso')
    parser.add_argument("--log_file", required=True, help="Dirección archivo de log")
    parser.add_argument("--subprocess", required=True, help="Subproceso a ejecutar", default="clean", choices=["clean", "train", "predict"])
    parser.add_argument("--fecha_inicial", required=True, help="Fecha Inicial para los datos")
    parser.add_argument("--fecha_final", required=True, help="Fecha Final para los datos")
    args = parser.parse_args()
    subprocess=args.subprocess
    log_file = args.log_file
    fecha_inicial = args.fecha_inicial
    fecha_final = args.fecha_final
    
    try:
        process(log_file, subprocess, fecha_inicial, fecha_final)
    
    except Exception as e:
        raise Exception(e)