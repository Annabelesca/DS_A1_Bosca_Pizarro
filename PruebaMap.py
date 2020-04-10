"""
Simple PyWren example using the map method.
In this example the map() method will launch one
map function for each entry in 'iterdata'. Finally
it will print the results for each invocation with
pw.get_result()
"""
import pywren_ibm_cloud as pywren
from Matriz import Matriz
import json
from datetime import datetime


def my_map_function(id, x):
    print("I'm activation number {}".format(id))
    return x + 7

def crearFicheroMatriz(fil,col,bucket_name, key,ibm_cos):
    print("Creando matriz de "+str(fil)+"x"+str(col))
    mat = Matriz(fil,col)
    ibm_cos.put_object(Bucket=bucket_name, Key=key,Body=json.dumps(mat.__dict__))
    #cos = COSBackend()
    #cos.put_object(bucket_name, key,json.dumps(mat.__dict__))
    return "Se ha creado matriz de "+str(fil)+"x"+str(col)+" y se ha subido al COS con el nombre de "+key

def prueba(id,fil,col):
    return "Se ha creado matriz de "+str(fil)+"x"+str(col)

if __name__ == "__main__":

    iterdata = [[2000,2000,'originalmatrix','MatrizA.txt'],[2000,2000,'originalmatrix','MatrizB.txt']]
    i_time=datetime.now()
    pw = pywren.ibm_cf_executor()
    pw.map(crearFicheroMatriz, iterdata)
    print(pw.get_result())
    f_time=datetime.now()
    print('Total elapsed time='+str(f_time-i_time))
    pw.clean()