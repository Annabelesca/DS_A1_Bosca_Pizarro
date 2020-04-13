<h1><p align="center">Map-Reduce</p></h1>
<h2><p align="center">Práctica 1. Sistemas Distribuidos</p></h1>

## 1. Introducción

En este proyecto se nos ha pedido que implementemos un algoritmo distribuido simple de multiplicación de matrices. Esta versión distribuida se basa en definir un número de workers que  trabajarán de forma concurrente en trozos (chunks) de las matrices a multiplicar.

Para la implementación de este programa, se nos ha pedido que utilicemos el IBM-PyWren. [PyWren](https://github.com/pywren/pywren) es un proyecto de código abierto que permite la ejecución de código Python a escala sobre las funciones de la nube de IBM, es decir, la plataforma de funciones como servicio (FaaS) de IBM, basada en Apache OpenWhisk. PyWren entrega el código del usuario en la plataforma sin servidores sin necesidad de conocer cómo se invocan y ejecutan las funciones. Soporta una API básica de MapReduce.
Para llevar el control de versiones del programa se nos ha pedido que usemos GitHub. El repositorio que contiene los ficheros de esta práctica es el siguiente: https://github.com/Annabelesca/DS_A1_Bosca_Pizarro


## 2. Estructura de la práctica

En este apartado, discutiremos los principales elementos estructurales de la práctica que básicamente recae en dos ficheros: Fichero MultMatriz.py, que se encarga de ejecutar el programa principal y el fichero Matriz.py que es un fichero que contiene la definición del objeto matriz. Después también cabe mencionar el fichero .pywren_config, este archivo permite configurar pywren y da acceso a las Cloud Function y al Cloud Storage de IBM. Por cuestiones de privacidad, no se sube el archivo con los datos de IBM que se han estado utilizando para realizar la práctica.

La práctica tiene varias secciones bastante diferenciadas, cuyas funciones las veremos a fondo en los apartados siguientes
*	Comprobación de los parámetros introducidos por el usuario (que no haya un número de filas/columnas inferior a 1, que el número de workers no exceda al número de filas de la matriz A y que el número de workers no exceda los 100).
*	La creación de matrices (su creación, inicialización y posterior subida al COS de IBM).
*	División de chunks en función del número de workers.
*	Llamada a la función map, que se encarga de procesar cada uno de los chunks a partir de los archivos que contienen las matrices en el COS de manera concurrente y generar un archivo con los resultados temporales.
*	Llamada a la función reduce, que se encarga de hacer un join de estos resultados temporales, creando la matriz resultante y subiéndola al COS. 
*	Llamada a la función de borrar archivos temporales creados durante el proceso de multiplicación

Otro asunto que no está de más mencionar es que nuestro COS está compuesto por dos buckets, uno llamado “originalmatrix” que contiene las dos matrices iniciales (ya sean creadas por nuestro programa o que hayan sido suministradas por el usuario) y otro que se llama “temporalresults” que contiene los ficheros con los resultados temporales de cada uno de los workers. Se ha decidido que el bucket llamado “temporalresults” sólo contendrá los ficheros temporales, por lo que el fichero creado como resultado de nuestro algoritmo se almacenará en el bucket “originalmatrix” bajo el nombre de “Matriz C.txt”. 


### 2.1 Matriz.py

Es la clase que define el objeto matriz en la cual se basa la práctica. Este objeto se encarga de crear matrices y de almacenarlas, así como de almacenar su número de filas y columnas. Además de crear las matrices, las inicializa con valores aleatorios en el rango [-10, 10]. También tiene un método para asignar valores en posiciones determinadas.
En caso de que sea necesario, las matrices creadas por el algoritmo se inicializarán en el constructor de este objeto. No obstante, lo harán en la nube a través de un map (se llama a un map en vez de un call_async por el hecho de crear las dos matrices de forma concurrente).


### 2.2 MultMatriz.py

Es la clase que se encarga de ejecutar el programa principal. Como nuestro algoritmo permite crear matrices y después dividir las matrices en diferentes chunks para paralelizar la multiplicación, la ejecución de este fichero requiere de algunos parámetros de entrada que se pasan al programa por la comanda para ejecutarlo:
<p align="center">>python MultMatriz.py m n l w</p>

<p><strong>Parámetros de entrada:</strong></p>

- m: Corresponderá a las filas de la matriz A a crear.
  
- n: Corresponderá a las columnas  de la matriz A y filas de B a crear.
  
- l: Corresponderá a las columnas de la matriz B a crear.
  
- w: Número de workers que usará nuestro programa.


<p><strong>Funciones:</strong></p>

* <strong>crearFicheroMatriz:</strong> Recibe por parámetro el número de filas y columnas con las que se desea crear la matriz y, además, el nombre con el que se quiere guardar en el COS de IBM.
  
* <strong>multMat:</strong> Nuestra función de map. Recibe por parámetro el número de línea de la matriz donde el worker debe empezar a trabajar y acabar. Cada worker que ejecuta esta función, baja los dos ficheros del COS y, a partir de una función auxiliar (multiplicacionMatrices, que veremos más adelante) se encarga de realizar la multiplicación de aquellas casillas que le corresponde. Cuando ha creado la matriz resultante, la sube al COS con el nombre “tempXXX” donde XXX corresponde a un número de 3 dígitos que indica la fila de inicio de la matriz calculada. Retorna este nombre.
  
* <strong>multiplicacionMatrices:</strong> Función auxiliar que recibe una lista de listas, representando al conjunto de filas a trabajar de la matriz A y un diccionario que contiene las filas y columnas pertinentes para hacer la multiplicación de estas dos matrices. Retorna un diccionario que contiene la matriz con el resultado de la multiplicación realizada.
  
* <strong>reduceFunction:</strong> Nuestra función de reduce. Se encarga de juntar todos los ficheros temporales para generar una matriz resultado. Recibe una lista ordenada de los nombres de los ficheros que se han generado en la función de map. Si esta lista tiene longitud 1 es que sólo había un worker y, por tanto, el fichero nombrado temp000.txt será el fichero que contenga toda la matriz resultante por tanto, se modifica el nombre de éste a MatrizC.txt y se sube al COS, al bucket “originalmatrix”. En caso que esta longitud no sea 1 significará  que hay más de un fichero temporal y la función hará el join de las submatrices que contiene cada uno de estos ficheros temporales. 
  
* <strong>resetBucket:</strong> Función que recibe como parámetro el nombre del bucket y el prefijo de los archivos a borrar. Se utiliza para borrar los ficheros temporales generados en la subdivisión de la matriz y aquellos resultantes de la serialización realizada por pickle.


