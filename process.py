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
                    sepp_model.predict_model(fecha_inicial_pr, fecha_final_pr)
                    prediccion = sepp_mod.predict_model(fecha_inicial_pr, fecha_final_pr)
                    array_cells_events_tst_data_cells = arr_cells_events_data(datos_eventos, prediccion[1]) 
                    fil = filtering_data(20, array_cells_events_tst_data_cells, prediccion[1], prediccion[0], fecha_inicial_pr)
                else:
                    datos_eventos = gpd.read_file('eventos_covariados.geojson')
                    file = open("fechas_entrenamiento.tx", "w")
                    def FECHA_mod(txt):
                        return txt.replace("T"," ")
                    file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[0])) + '\n')
                    file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[-1])) + '\n')
                    file.close()
                    sepp_model.train_model(datos_eventos)
                    prediccion = sepp_mod.predict_model(fecha_inicial_pr, fecha_final_pr)
                    array_cells_events_tst_data_cells = arr_cells_events_data(datos_eventos, prediccion[1]) 
                    fil = filtering_data(20, array_cells_events_tst_data_cells, prediccion[1], prediccion[0], fecha_inicial_pr)            
            else:
                filename = "fechas_entrenamiento.txt"
                parametros = np.array([])
                with open(filename) as f_obj:
                    for line in f_obj:
                        parametros = np.append(parametros, str(line.rstrip()))
                fecha_inicial_tr = parametros[0]
                fecha_final_tr = parametros[1]
                print(fecha_inicial_tr, fecha_final_tr, fecha_inicial_pr, fecha_final_pr)
                date_format_str = '%Y-%m-%d %H:%M:%S'
                fecha_final_tr = datetime.strptime(fecha_final_tr, date_format_str)
                fecha_inicial_pr = datetime.strptime(fecha_inicial_pr, date_format_str)
                diff_pr = (fecha_inicial_pr - fecha_final_tr).total_seconds()/3600
                print
                if diff_pr < 336:
                    print(1)
                    prediccion = sepp_mod.predict_model(fecha_inicial_pr, fecha_final_pr)
                    print(2)
                    array_cells_events_tst_data_cells = arr_cells_events_data(datos_eventos, prediccion[1]) 
                    print(3)
                    fil = filtering_data(20, array_cells_events_tst_data_cells, prediccion[1], prediccion[0], fecha_inicial_pr)            
                    print("Se completó el proceso de prediccion")
                
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