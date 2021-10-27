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

def process(log_file, sub_process, fecha_inicial, fecha_final, fecha_inicial_pr, fecha_final_pr):
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
                file = open("fechas_entrenamiento.txt", "w")
                file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[0])) + '\n')
                file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[-1])) + '\n')
                file.close()
                sepp_model.train_model(datos_eventos)
        
        elif subprocess == "predict":
            if os.path.exists('./parametros_optimizados.txt') == False:
                print("Primero se debe hacer el proceso de train")
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
                    file = open("fechas_entrenamiento.txt", "w")
                    file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[0])) + '\n')
                    file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[-1])) + '\n')
                    file.close()
                    sepp_model.train_model(datos_eventos)
            else:
                filename = "fechas_entrenamiento.txt"
                with open(filename) as f_obj:
                    for line in f_obj:
                        parameters = np.append(parameters,str(line.rstrip()))
                fecha_inicial_tr = parameters[0]
                fecha_final_tr = parameters[1]
                sepp_model.prediction_model(fecha_inicial_pr, fecha_final_pr) 
                       
    except Exception as e:
        msg_error = "No se completó función process"
        logging.error(msg_error)
        raise Exception(msg_error + " / " +str(e))                 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Función ejecución proceso')
    parser.add_argument("--log_file", required=True, help="Dirección archivo de log")
    parser.add_argument("--subprocess", required=True, help="Subproceso a ejecutar", default="clean", choices=["clean", "train", "predict"])
    parser.add_argument("--fecha_inicial", required=True, help="Fecha Inicial para los procesos de clean y train")
    parser.add_argument("--fecha_final", required=True, help="Fecha Final para los procesos de clean y train")
    parser.add_argument("--fecha_inicial_pr", required=False, help="Fecha Inicial para el proceso de prediccion")
    parser.add_argument("--fecha_final_pr", required=False, help="Fecha Final para el proceso de prediccion")
    args = parser.parse_args()
    subprocess=args.subprocess
    log_file = args.log_file
    fecha_inicial = args.fecha_inicial
    fecha_final = args.fecha_final
    fecha_inicial_pr = args.fecha_inicial_pr
    fecha_final_pr = args.fecha_final_pr

    try:
        process(log_file, subprocess, fecha_inicial, fecha_final, fecha_inicial_pr, fecha_final_pr)
    
    except Exception as e:
        raise Exception(e)