import pywren_ibm_cloud as pywren
from Matriz import Matriz
import json
from datetime import datetime
import sys

#from cos_backend import COSBackend


def crearFicheroMatriz(fil,col,bucket_name, key,ibm_cos):
    print("Creando matriz de "+str(fil)+"x"+str(col))
    mat = Matriz(fil,col)
    ibm_cos.put_object(Bucket=bucket_name, Key=key,Body=json.dumps(mat.__dict__))
    #cos = COSBackend()
    #cos.put_object(bucket_name, key,json.dumps(mat.__dict__))
    return "Se ha creado matriz de "+str(fil)+"x"+str(col)+" y se ha subido al COS con el nombre de "+key

def leerMatriz(bucket_name, key,ibm_cos):
    result= ibm_cos.get_object(Bucket=bucket_name, Key=key)['Body'].read()
    #cos = COSBackend()
    #result=cos.get_object(bucket_name, key)
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
        iterdata = [[m,n,'originalmatrix','MatrizA.txt'],[n,l,'originalmatrix','MatrizB.txt']]
        pw = pywren.ibm_cf_executor()
        pw.map(crearFicheroMatriz, iterdata)
        pw.get_result()
        
        #Una vez tenemos las matrices creadas y almacenadas en el COS, procedemos a su division en los diferentes chunks para relizar la
        #multiplicacion --> Map


        #Una vez acabe la ejecucion de todos los workers, 