import requests
import traceback
import json
import constants_manager as c
from datetime import datetime
from pyspark.sql import SparkSession, functions
import logging
import pandas as pd
from datetime import timedelta, datetime

def get_token_acces():
    """
    Obtiene un token para realizar operaciones en el API
    :return: string token
    """
    token_response = requests.post(c.API_HOST + ':' + c.API_PORT + c.API_RELATIVE_PATH_TOKEN_ACCESS, data={'username' : c.API_USER, 'password' : c.API_PASSWORD})
    token = token_response.json()["auth_token"]
    return token

def get_tipos_proceso(token):
    """
    Obtiene la información de los tipos de proceso de la API
 
    :param token: string token
    :return: dict con keys los tipos de procesos y valores los ids en el API
    """
    auth_header={'Authorization' : 'Token ' + token}
    response = requests.get(c.API_HOST + ':' + c.API_PORT + c.API_RELATIVE_PATH_GET_TIPOPROCESO, headers=auth_header)
    dict_response = response.json()
    tipos_proceso={c.NAME_PREPROCESAMIENTO:0,c.NAME_ENTRENAMIENTO:0,c.NAME_PREDICCION:0,c.NAME_VALIDACION:0}
    for i in dict_response:
        if i["nombre_tipo_proceso"] in tipos_proceso:
            tipos_proceso[i["nombre_tipo_proceso"]]=i['id_tipo_proceso']
    return tipos_proceso
    
def get_estados_ejecucion(token):
    """
    Obtiene la información de los estados de ejecución de la API
 
    :param token: string token
    :return: dict con keys los estados de procesos y valores los ids en el AP
    """
    auth_header={'Authorization' : 'Token ' + token}
    response = requests.get(c.API_HOST + ':' + c.API_PORT + c.API_RELATIVE_PATH_GET_ESTADOEJECUCION, headers=auth_header)
    dict_response = response.json()

    estados={c.ESTADO_EXITO:0,c.ESTADO_ERROR:0,c.ESTADO_PROCESO:0,c.ESTADO_CANCELADO:0}
    for i in dict_response:
        if i["nombre_estado_ejecucion"] in estados:
            estados[i["nombre_estado_ejecucion"]]=i['id_estado_ejecucion']
    return estados


def update_process_state(id_tipo_proceso, id_estado_ejecucion, token):
    """
    Actualiza un estado de proceso
 
    :param id_tipo_proceso: id del tipo de proceso a actualizar
    :param id_estado_ejecucion: id del estado de ejecucion del proceso a actualizar
    :param token: string token
    :return:
    """
    fecha_actual = datetime.now()
    time_stamp = fecha_actual.strftime('%Y-%m-%-d %H:%M:%S')
    auth_header={'Authorization' : 'Token ' + token}
    try:
        data = {'fecha_hora_proceso' : time_stamp, 'usuario_ejecucion' : c.USUARIO_EJECUCION, 'ip_ejecucion' : c.IP_EJECUCION, 'id_tipo_proceso' : id_tipo_proceso, 'id_estado_ejecucion' : id_estado_ejecucion}
        #print(data)
        response = requests.post(c.API_HOST + ':' + c.API_PORT + c.API_RELATIVE_PATH_UPDATE_PROCESS_STATE, headers=auth_header, data=data)
        response.raise_for_status()
        #print(response.json())
        logging.debug("Actualización estado de proceso en el API realizada")
    except requests.exceptions.HTTPError as error:
        #print(error)
        traceback.print_exc()
        logging.debug("No se completo la actualización estado de proceso en el API")