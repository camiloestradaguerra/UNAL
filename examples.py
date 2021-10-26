from process import *
import os
import timeit



def example1():
    print("Ejemplo primera ejecución cuando no existe modelo preentrenado y si existen los " +
          "archivos de los modelos basicos de clasificación y cuantificación. Al final " +
          "guarda todos los modelos creados"
          )
    os.system("python process.py " + "--log_file 'example_log.log' " + "--subprocess 'clean' " + "--fecha_inicial '2021-01-01 00:00:00' " + "--fecha_final '2021-01-05 23:59:59' ")

example1()