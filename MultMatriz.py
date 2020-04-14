import pywren_ibm_cloud as pywren
import json
from datetime import datetime
import sys
try:
    from Matriz import Matriz
except:
    print("Error en el import del modulo Matriz. Asegurate que el fichero Matriz.py se encuentra en el mismo directorio que este."); exit(1)



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
    #return "Se ha creado matriz de "+str(fil)+"x"+str(col)+" y se ha subido al COS con el nombre de "+key


"""Funcion auxiliar que se encarga de multiplicar dos matrices
Parametros de entrada:
    matA - Lista de listas que representa al subconjunto de filas a trabajar
    matB - Diccionario que contiene la matriz y sus datos (nFilas y nColumnas)
Retorna:
    Diccionario que contiene la matriz con el resultado de la multiplicacion de las dos matrices de entrada
"""
def multiplicacionMatrices(matA, matB):
    resultado={'nFilas':len(matA), 'nColumns':matB['nColumns'], 'matriz': []}   #Definimos la matriz resultante como un diccionario. 
    #Tendra el mismo numero de filas que la matriz A y el mismo numero de columnas que la matriz B
    
    for i in range(len(matA)):     #Recorremos filas de la matriz A
        fila=[]                 
        for j in range(matB['nColumns']):   #Recorremos columnas de la matriz B
            suma=0
            for c in range(len(matA[0])):   #De cada columna de la matriz B, recorremos las filas
                suma+=matA[i][c]*(matB['matriz'][c][j]) #Incrementamos el indice del resultado de la casilla
            fila.append(suma)   #Guardamos el resultado en la fila
            
        resultado['matriz'].append(fila)    #Guardamos la fila entera en la matriz resultante

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
    ibm_cos.put_object(Bucket=bucketTemporal, Key='temp'+nFichero+'.txt',Body=json.dumps(resultado))   #Guardamos en el COS la submatriz calculada
    
    return 'temp'+nFichero+'.txt'

"""Funcion que se encarga vaciar el bucket en el que se almacenaran los archivos temporales de cada uno de los workers
Parametros de entrada:
    key - Nombre del bucket a vaciar
    ibm-cos - Cliente de COS ready-to-use
Retorna:
    String indicando que se ha finalizado el reseteo del bucket
"""
def resetBucket(key,prefix,ibm_cos):
    objects=ibm_cos.list_objects(Bucket=key,Prefix=prefix)    #Obtenemos los datos del bucket
    if 'Contents' in objects:                   #Si el bucket contiene ficheros, los borramos uno a uno
        for elements in objects['Contents']:
            nombre=elements.get('Key')
            ibm_cos.delete_object(Bucket=key,Key=nombre)

    #return ('Bucket temporal vacio')

"""Funcion que se encarga de juntar todos los ficheros temporales para calcular la matriz resultante
Parametros de entrada:
    results - Lista con los ficheros que contienen las multiplicaciones de las submatrices temporales
    ibm-cos - Cliente de COS ready-to-use
Retorna:
    String indicando que se ha finalizado la multiplicacion de las matrices orginales
"""
def reduceFunction(results,ibm_cos):
    resultado={'nFilas':0, 'nColumns':0, 'matriz': []}  #Creamos un diccionario que contendra la matriz resultante
    
    if (len(results)==1):       #Solo habia un worker que ya habra calculado la matriz -> No hara falta reduce
        matC = ibm_cos.get_object(Bucket=bucketTemporal, Key=results[0])['Body'].read()  #Obtenemos fichero de la matriz resultante
        ibm_cos.put_object(Bucket=bucketOriginal, Key='MatrizC.txt',Body=matC.decode())  #Guardamos los datos en el COS 

    else:        #Si habia mas de un worker, tenemos que juntar los ficheros que ha generado cada uno de estos
        for elementos in results:   #Recorremos lista generada por el map que contiene los nombres de los ficheros generados
            matC = ibm_cos.get_object(Bucket=bucketTemporal, Key=elementos)['Body'].read()  #Obtenemos fichero de la submatriz
            matC = json.loads(matC.decode())    #Pasamos el str resultante a diccionario para que sea mas facil operar con el
            if (resultado['nColumns']==0): resultado['nColumns']=int(matC['nColumns'])  #Inicializamos el numero de columnas (en todos los ficheros temporales son la misma)
            resultado['nFilas']=resultado['nFilas']+matC['nFilas']  #Actualizamos numero de filas
            for fila in matC['matriz']: #Leemos de cada fichero las fijas y las introducimos en la matriz resultante
                resultado['matriz'].append(fila)

            ibm_cos.put_object(Bucket=bucketOriginal, Key='MatrizC.txt',Body=json.dumps(resultado))   #Guardamos en el COS la matriz resultante  
        
    return "Matrices multiplicadas. Resultado almacenado en el Bucket: "+bucketOriginal+" con el nombre de MatrizC.txt"
    #return resultado


if __name__ == '__main__':
    #Si no se pasan los argumentos correctos, el programa finaliza (m, n, l y nWorkers)
    if(len(sys.argv)!=5):       
        print('Error, hay que pasar 4 parametros: m, n, l i numero de Workers')

    else:
        try:
            m=int(sys.argv[1]); n=int(sys.argv[2]); l=int(sys.argv[3]); nChunks=int(sys.argv[4])
        except:
            print("Error en el formato de los parametros. Revisalos"); exit(1)
        
        #Comprobamos que las dimensiones de las matrices sean correctas
        if(m<1 or n<1 or l< 1): print("Las dimensiones de una matriz deben ser superiores a 0. Terminando programa..."); exit(1)

        #Comprobamos que el numero de workers es almenos 1
        if(nChunks<1): print("Debe haber al menos un worker. Terminando programa..."); exit(1)
        
        #Numero maximo de workers debe ser 100, si hay mas, reducimos a este valor
        if (nChunks>100): nChunks=100; print("El numero de workers debe ser 100 como maximo. Numero de workers reducido a: "+str(nChunks))

        #Como vamos a multiplicar row-wise, numero maximo de chunks es igual al numero de filas de la primera matriz
        if (nChunks>m): nChunks=m; print("El numero de workers debe ser igual o inferior al numero de filas de la matriz A. Numero de workers reducido a: "+str(nChunks))

        pw = pywren.ibm_cf_executor()
        
        try:
            #Creamos matrices y las inicializamos de forma random (de -10 a 10)
            print("Procedemos a crear matrices:\n"+"\tMatriz A: "+str(m)+"x"+str(n)+"\n\tMatriz B: "+str(n)+"x"+str(l)+"\n")
            iterdata = [[m,n,'MatrizA.txt'],[n,l,'MatrizB.txt']]    
            matrices=pw.map(crearFicheroMatriz, iterdata)   #Con un map creamos de forma concurrente la matriz A y la matriz B
            pw.wait(matrices)
            pw.clean()
            print("Matrices creadas y almacenadas en el COS correctamente.\n")
        except:
            print("Ha habido un error en la creacion de matrices. Comprueba que los buckets existen."); exit(1)
        
        #Nos aseguramos que el bucket que va a contener los archivos temporales de nuestro programa esta vacio
        
        #pw = pywren.ibm_cf_executor()
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
        i_time=datetime.now()
        #Una vez se han creado los ficheros temporales, ha llegado el momento de juntarlos
        try:
            #m=pw.map(multMat,iterdata)
            m=pw.map_reduce(multMat, iterdata, reduceFunction)
            pw.wait(m)
            pw.clean()
            print("Matrices multiplicadas. Matriz resultante almacenada en el Bucket "+bucketOriginal+" con el nombre de Matrix C.txt")
        except:
            print("Un error ha ocurrido. Comprueba que el nombre de los buckets sea el correcto y de que haya matrices existentes.")
            exit(1)

        f_time=datetime.now()
        print('Tiempo de ejecucion de las matrices = '+str(f_time-i_time)+"\n")
        
        #Una vez generado el fichero que contiene la matriz C (matriz A * matriz B) borramos todos los archivos temporales que se hayan ido generando
        print("Procedemos a borrar los archivos temporales que se han creado: ")
        reset=pw.map(resetBucket,[[bucketTemporal,'temp'],[bucketOriginal,'pywren']])
        pw.wait(reset)
        pw.clean
        print("Archivos temporales borrados\n")
        
        
