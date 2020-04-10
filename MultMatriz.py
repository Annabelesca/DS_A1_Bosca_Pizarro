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
    mat = Matriz(fil,col)   #Llamamos al constructor de la clase Matriz para generar una matriz filxcol especificado en el fichero de entrada, inicializada con valores entre -10 y 10
    ibm_cos.put_object(Bucket=bucketOriginal, Key=key,Body=json.dumps(mat.__dict__)) #Guardamos en el COS la matriz creada bajo el nombre especificado en los ficheros de entrada
    return "Se ha creado matriz de "+str(fil)+"x"+str(col)+" y se ha subido al COS con el nombre de "+key


"""Funcion auxiliar que se encarga de multiplicar dos matrices
Parametros de entrada:
    matA - Lista de listas que representa al subconjunto de filas a trabajar
    matB - Diccionario que contiene la matriz y sus datos (nFilas y nColumnas)
Retorna:
    Objeto matriz con el resultado de la multiplicacion de las dos matrices de entrada
"""
def multiplicacionMatrices(matA, matB):
    resultado = Matriz(len(matA),matB['nColumns'])  
    for i in range(len(matA)):                      
        for j in range(matB['nColumns']):
            suma=0
            for c in range(len(matA[0])):
                suma+=matA[i][c]*(matB['matriz'][c][j])
            resultado.asignarValor(i,j,suma)
    return resultado


"""Funcion que se encarga de guardar en el COS la multiplicacion de la matriz de un worker
Parametros de entrada:
    inic - Indice que marca desde que fila se realizara la multiplicacion de la matriz
    fin - Indice que marca la fila en la que terminará la multiplicacion de la matriz
    ibm-cos - Cliente de COS ready-to-use
Retorna:
    Objeto matriz con el resultado de la multiplicacion de las dos matrices de entrada
"""
def multMat(inic,fin,ibm_cos):
    matA = ibm_cos.get_object(Bucket=bucketOriginal, Key='MatrizA.txt')['Body'].read()  #Obtenemos fichero de la matriz de A
    matA = json.loads(matA)['matriz']
    matA = matA[inic:(fin+1)]   #Seleccionamos submatriz de interes

    matB = ibm_cos.get_object(Bucket=bucketOriginal, Key='MatrizB.txt')['Body'].read() #Obtenemos fichero de la matriz de B
    matB = json.loads(matB) #Cargamos el diccionario que representa la matriz B

    resultado=multiplicacionMatrices(matA,matB) #Llamamos a la funcion auxiliar para que haga la multiplicacion de las dos matrices
    nFichero= '%03d' % inic #Queremos que nuestro fichero quede almacenado en el cos, marcando con tres digitos la fila por la que comienzan - temp001.txt, por ejemplo
    ibm_cos.put_object(Bucket=bucketTemporal, Key='temp'+nFichero+'.txt',Body=json.dumps(resultado.__dict__))   #Guardamos en el COS la submatriz calculada


"""Funcion que se encarga vaciar el bucket en el que se almacenaran los archivos temporales de cada uno de los workers
Parametros de entrada:
    key - Nombre del bucket a vaciar
    ibm-cos - Cliente de COS ready-to-use
Retorna:
    String indicando que se ha finalizado el reseteo del bucket
"""
def resetBucketTemporal(key,ibm_cos):
    objects=ibm_cos.list_objects(Bucket=key)    #Obtenemos los datos del bucket
    if 'Contents' in objects:                   #Si el bucket contiene ficheros, los borramos uno a uno
        for elements in objects['Contents']:
            nombre=elements.get('Key')
            ibm_cos.delete_object(Bucket=key,Key=nombre)

    return ('Bucket temporal vacio')

if __name__ == '__main__':
    #Si no se pasan los argumentos correctos, el programa finaliza (m, n, l y nWorkers)
    if(len(sys.argv)!=5):       
        print('Error, s\'han de pasar 4 parametres: m, n, l i numero de Workers')

    else:
        m=int(sys.argv[1]); n=int(sys.argv[2]); l=int(sys.argv[3]); nChunks=int(sys.argv[4])
        
        #Comprobamos que las dimensiones de las matrices sean correctas
        if(m<1 or n<1 or l< 1): print("Les dimensions de una matriu han de ser superiors a 0"); exit(1)
        
        #Numero maximo de workers debe ser 100, si hay mas, reducimos a este valor
        if (nChunks>100): nChunks=100; print("Nombre de chunks reduit a: "+str(nChunks))

        #Como vamos a multiplicar row-wise, numero maximo de chunks es igual al numero de filas de la primera matriz
        if (nChunks>l): nChunks=l; print("Nombre de chunks reduit a: "+str(nChunks))

        pw = pywren.ibm_cf_executor()
        
        #Creamos matrices y las inicializamos de forma random (de -10 a 10)
        print("Procedemos a crear matrices:\n"+"\tMatriz A: "+str(m)+"x"+str(n)+"\n\tMatriz B: "+str(n)+"x"+str(l)+"\n")
        iterdata = [[m,n,'MatrizA.txt'],[n,l,'MatrizB.txt']]    
        matrices=pw.map(crearFicheroMatriz, iterdata)   #Con un map creamos de forma concurrente la matriz A y la matriz B
        pw.wait(matrices)
        pw.clean()
        print("Matrices creadas y almacenadas en el COS correctamente\n")
        
        #Nos aseguramos que el bucket que va a contener los archivos temporales de nuestro programa esta vacio
        print("Procedemos a vaciar el bucket: "+bucketTemporal+"\n")
        pw.call_async(resetBucketTemporal,bucketTemporal)
        pw.wait()
        pw.clean
        print("Bucket temporal vaciado\n")

        i_time=datetime.now()
        #Una vez tenemos las matrices creadas y almacenadas en el COS, procedemos a su division en los diferentes chunks para relizar la multiplicacion --> Map
        filasPerWorker = int(m/nChunks)     
        elementosRestantes = m%nChunks          #Calculamos cuantas filas va a procesar cada worker
        
        #print(filasPerWorker)
        iterdata=[]
        
        j=0; i=0
        while(j<elementosRestantes):    #Asignamos elementos que faltan por asignar a workers (balancear carga)
            chunk=[i,(i+filasPerWorker)]
            iterdata.append(chunk)
            j=j+1
            i=i+filasPerWorker+1

        while(i<m):                     #Acabamos de Asignamos los elementos
            chunk=[i,(i+filasPerWorker)-1]
            iterdata.append(chunk)
            i=i+filasPerWorker

        #print(iterdata)
        print("Chunks creados\n")
        #Una vez tenemos los chunks asignados, llamamos a la funcion Map que se encargara de que se realicen las multiplicaciones de forma concurrente
        matC=pw.map(multMat, iterdata)
        pw.wait(matC)
        print("Archivos temporales creados y almacenados en el COS\n")

        #Una vez se han creado los ficheros temporales, ha llegado el momento de juntarlos
        
        f_time=datetime.now()
        print('Total elapsed time='+str(f_time-i_time)+"\n")