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
            # Si NO hay hay datos almacenados: Revisa si existe el df con datos y si NO, entonces crea un df 
            #con los datos entre las fechas seleccionadas y entrena el modelo con estos datos
            if os.path.exists('./eventos_covariados.geojson') == False:
                print("Primero se debe hacer el proceso de Preprocesamiento de Datos.")
                # Procesa los datos
                datos_eventos = sepp_model.preprocdatos_model(fecha_inicial, fecha_final)
                # Crea un archivo externo con las fechas seleccionadas para los datos
                file = open("fechas_entrenamiento.txt", "w")
                file.write(str(fecha_inicial) + '\n')
                file.write(str(fecha_final) + '\n')
                file.close()
                # Entrena el modelo y crea un archivo con los parametros optimizados
                sepp_model.train_model(datos_eventos)
            # Si SI hay un modelo entrenado: Carga los datos y entrena con los datos que hayan en 
            # eventos_covariados y crea un archivo externo con las fechas inicial y final de estos datos
            else:
                # Lee los el df con los datos
                datos_eventos = gpd.read_file('eventos_covariados.geojson')
                # Crea el archivo externo con las fechas inicial y final de los datos
                file = open("fechas_entrenamiento.txt", "w")
                file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[0])) + '\n')
                file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[-1])) + '\n')
                file.close()
                # Entrena el modelo con los datos y crea un archivo con los parametros optimizados
                sepp_model.train_model(datos_eventos)
        
        elif subprocess == "predict":
            # Si NO hay un modelo ya previamente entrenado: Revisa si existe el archivo parametros_optimizados
            # (NO). Luego revisa si están por lo menos los datos eventos_covariados, y si no estan, entonces
            # hace un procesamiento de datos entre las fechas seleccionadas, entrena el modelo con estas fechas
            # y luego predice con las fechas ingresadas por el usuario
            if os.path.exists('./parametros_optimizados.txt') == False:
                print("Primero se debe hacer el proceso de train")
                # Lo mismo que para el proceso de entrenamiento
                if os.path.exists('./eventos_covariados.geojson') == False:
                    print("Primero se debe hacer el proceso de Preprocesamiento de Datos.")
                    sepp_model.preprocdatos_model(fecha_inicial, fecha_final)
                    datos_eventos = gpd.read_file('eventos_covariados.geojson')
                    file = open("fechas_entrenamiento.txt", "w")
                    file.write(str(fecha_inicial) + '\n')
                    file.write(str(fecha_final) + '\n')
                    file.close()
                    sepp_model.train_model(datos_eventos)
                    #sepp_model.predict_model(fecha_inicial_pr, fecha_final_pr)
                    prediccion = sepp_model.predict_model(fecha_inicial_pr, fecha_final_pr)
                    array_cells_events_tst_data_cells = arr_cells_events_data(datos_eventos, prediccion[1]) 
                    fil = filtering_data(20, array_cells_events_tst_data_cells, prediccion[1], prediccion[0], fecha_inicial_pr)
                    file = open("fechas_prediccion.txt", "w")
                    file.write(str(fecha_inicial_pr) + '\n')
                    file.write(str(fecha_final_pr) + '\n')
                    file.close()
                else:
                    datos_eventos = gpd.read_file('eventos_covariados.geojson')
                    file = open("fechas_entrenamiento.tx", "w")
                    def FECHA_mod(txt):
                        return txt.replace("T"," ")
                    file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[0])) + '\n')
                    file.write(FECHA_mod(str(datos_eventos.FECHA.iloc[-1])) + '\n')
                    file.close()
                    sepp_model.train_model(datos_eventos)
                    prediccion = sepp_model.predict_model(fecha_inicial_pr, fecha_final_pr)
                    array_cells_events_tst_data_cells = arr_cells_events_data(datos_eventos, prediccion[1]) 
                    fil = filtering_data(20, array_cells_events_tst_data_cells, prediccion[1], prediccion[0], fecha_inicial_pr)            
                    file = open("fechas_prediccion.txt", "w")
                    file.write(str(fecha_inicial_pr) + '\n')
                    file.write(str(fecha_final_pr) + '\n')
                    file.close()
            # Si SI hay un modelo ya previamente entrenado: Revisa si existe el archivo parametros_optimizados
            # (NO). Luego revisa si están por lo menos los datos eventos_covariados, y si no estan, entonces
            # hace un procesamiento de datos entre las fechas seleccionadas, entrena el modelo con estas fechas
            # y luego predice con las fechas ingresadas por el usuario
            else:
                # Carga de los datos con los que se entreno el modelo
                datos_eventos = gpd.read_file('eventos_covariados.geojson')
                # Se leen las fechas inicial y final con las que se entreno el modelo. Si hay una diferencia mayor
                # a 2 semanas entre la fecha inicial de prediccion y la fecha final del entrenamiento, entonces 
                # toca entrenar con datos nuevos
                filename = "fechas_entrenamiento.txt"
                parametros = np.array([])
                with open(filename) as f_obj:
                    for line in f_obj:
                        parametros = np.append(parametros, str(line.rstrip()))
                fecha_inicial_tr = parametros[0]
                fecha_final_tr = parametros[1]
                print(fecha_inicial_tr, fecha_final_tr, fecha_inicial_pr, fecha_final_pr)
                date_format_str = '%Y-%m-%d %H:%M:%S'
                fecha_final_tr1 = datetime.strptime(fecha_final_tr, date_format_str)
                fecha_inicial_pr1 = datetime.strptime(fecha_inicial_pr, date_format_str)
                diff_pr = (fecha_inicial_pr1 - fecha_final_tr1).total_seconds()/3600
                print(type(diff_pr))
                if diff_pr < 336.0:
                    # Se hace la prediccion
                    prediccion = sepp_model.predict_model(fecha_inicial_pr, fecha_final_pr)
                    array_cells_events_tst_data_cells = arr_cells_events_data(datos_eventos, prediccion[1]) 
                    # Almacena el df con eventos unicamente en los puntos calientes
                    fil = filtering_data(20, array_cells_events_tst_data_cells, prediccion[1], prediccion[0], fecha_inicial_pr)            
                    #validation = sepp_mod.validation_model(fil[0], fil[1])
                    file = open("fechas_prediccion.txt", "w")
                    file.write(str(fecha_inicial_pr) + '\n')
                    file.write(str(fecha_final_pr) + '\n')
                    file.close()
                    file = open("datos_validacion.txt", "w")
                    file.write(str(fil[0]) + '\n')
                    file.write(str(fil[1]) + '\n')
                    file.close()

        elif subprocess == "validation":
            ya = datetime.now()
            date_format_str = '%Y-%m-%d %H:%M:%S'
            fecha_final_val = datetime.strptime(fecha_final, date_format_str)
            ts_ya = datetime.timestamp(ya)
            ts_fecha_final_val = datetime.timestamp(fecha_final_val)
            diff_val = (ts_ya - ts_fecha_final_val)
            if diff_val > 0:
                if os.path.exists('./datos_validacion.txt') == False:
                    print("Primero se debe hacer el proceso de prediccion")    
                else:
                    filename = "fechas_prediccion.txt"
                    parametros = np.array([])
                    with open(filename) as f_obj:
                        for line in f_obj:
                            parametros = np.append(parametros, str(line.rstrip()))
                    f_inicial_pr = datetime.strptime(parametros[0], date_format_str)
                    f_final_pr = datetime.strptime(parametros[1], date_format_str)
                    f_inicial_val = datetime.strptime(fecha_inicial, date_format_str)
                    f_final_val = datetime.strptime(fecha_final, date_format_str)
                    if f_inicial_pr == f_inicial_val and f_final_pr == f_final_val:
                        filename = "datos_validacion.txt"
                        par = np.array([])
                        with open(filename) as f_obj:
                            for line in f_obj:
                                par = np.append(par, str(line.rstrip()))
                        par=np.asfarray(par,float)
                        validacion = sepp_model.validation_model(par[0], par[1])                
    except Exception as e:
        msg_error = "No se completó función process"
        logging.error(msg_error)
        raise Exception(msg_error + " / " +str(e))                 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Función ejecución proceso')
    parser.add_argument("--log_file", required=True, help="Dirección archivo de log")
    parser.add_argument("--subprocess", required=True, help="Subproceso a ejecutar", default="clean", choices=["clean", "train", "predict", "validation"])
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