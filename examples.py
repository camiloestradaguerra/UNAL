from process import *
import os
import timeit
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import shutup 
shutup.please()


def example1():
    print("Ejemplo para el procesamiento de los datos: Extracción de los datos de la API, limpieza y preparacion de los datos para el proceso entrenamiento")
    os.system("python process.py " + "--log_file 'example_log.log' " + "--subprocess 'clean' " + "--fecha_inicial '2021-01-01 00:00:00' " + "--fecha_final '2021-01-05 23:59:59' ")

def example2():
    print("Ejemplo para el proceso de entrenamiento")
    os.system("python process.py " + "--log_file 'example_log.log' " + "--subprocess 'train' " + "--fecha_inicial '2021-01-01 00:00:00' " + "--fecha_final '2021-01-03 13:59:59' ")

def example3():
    print("Ejemplo para el proceso de prediccion")
    os.system("python process.py " + "--log_file 'example_log.log' " + "--subprocess 'predict' " + "--fecha_inicial '2021-01-01 00:00:00' " + "--fecha_final '2021-01-03 13:59:59' " + "--fecha_inicial_pr '2021-01-04 00:00:00' " + "--fecha_final_pr '2021-01-04 09:59:00' ")

def example4():
    print("Ejemplo para el proceso de validacion")
    os.system("python process.py " + "--log_file 'example_log.log' " + "--subprocess 'validation' " + "--fecha_inicial '2021-01-04 00:00:00' " + "--fecha_final '2021-01-04 09:59:00' ")

example4()
