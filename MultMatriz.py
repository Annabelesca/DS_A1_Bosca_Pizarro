import pywren_ibm_cloud as pywren
from Matriz import Matriz
import json
from datetime import datetime
import sys


bucketOriginal='originalmatrix'     #Nombre del bucket que contendra las 2 matrices originales y el resultado final de la multiplicacion 
bucketTemporal='temporalresults'    #Nombre del bucket que contendra los resultados temporales de cada worker



"""Funcion que se encarga de crear una matriz e inicializarla aleatoriamente con valores comprendidos entre -10 y 10 y subirla al COS
Parametros de entrada:
    fil - Numero de filas de la matriz
    col - Numero de columnas de la matriz
    key - Nombre del fichero en el COS que contendra la matriz
    ibm-cos - Cliente de COS ready-to-use
Retorna:
    String indicando que la matriz se ha creado correctamente y el nombre con el cual se ha guardado
"""
def crearFicheroMatriz(fil,col, key,ibm_cos):
    print("Creando matriz de "+str(fil)+"x"+str(col))
    mat = Matriz(fil,col)
    ibm_cos.put_object(Bucket=bucketTemporal, Key=key,Body=json.dumps(mat.__dict__))
    return "Se ha creado matriz de "+str(fil)+"x"+str(col)+" y se ha subido al COS con el nombre de "+key


"""Funcion que se encarga de obtener una matriz almacenada en un fichero en el COS a partir de su nombre
Parametros de entrada:
    key - Nombre del fichero en el COS que contendra la matriz
    ibm-cos - Cliente de COS ready-to-use
Retorna:
    Diccionario que contiene la matriz y sus datos (nFilas y nColumnas)
"""
def leerMatriz(key,ibm_cos):
    result= ibm_cos.get_object(Bucket=bucketTemporal, Key=key)['Body'].read()
    return json.loads(result)





if __name__ == '__main__':
    #Si no se pasan los argumentos correctos, el programa finaliza (m, n, l y nWorkers)
    if(len(sys.argv)!=5):       
        print('Error, s\'han de pasar 4 parametres: m, n, l i numero de Workers')

    else:
        m=int(sys.argv[1]); n=int(sys.argv[2]); l=int(sys.argv[3]); nChunks=int(sys.argv[4])

        #Comprobamos que las dimensiones de las matrices sean correctas
        if(m<1 or n<1 or l< 1): print("Les dimensions de una matriu han de ser superiors a 0"); exit(1)
        
        #Como vamos a multiplicar row-wise, numero maximo de chunks es igual al numero de filas de la primera matriz
        if (nChunks>l): nChunks=l; print("Nombre de chunks reduit a: "+nChunks)

        #Creamos matrices y las inicializamos de forma random (de -10 a 10)
        iterdata = [[m,n,'MatrizA.txt'],[n,l,'MatrizB.txt']]
        pw = pywren.ibm_cf_executor()
        pw.map(crearFicheroMatriz, iterdata)
        print(pw.get_result())
        pw.clean()

        
        #Una vez tenemos las matrices creadas y almacenadas en el COS, procedemos a su division en los diferentes chunks para relizar la
        #multiplicacion --> Map


        #Una vez acabe la ejecucion de todos los workers, 