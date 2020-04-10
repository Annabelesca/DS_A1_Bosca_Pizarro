import random
import json

class Matriz:

    nFilas=0
    nColumns=0
    matriz=None

    def __init__(self,filas,columnas):
        self.nFilas=filas
        self.nColumns=columnas
        self.matriz=[]
        for i in range(filas):
            fila=[]
            for j in range(columnas):
                randomNum = random.randrange(20)                   #Buscamos numero random de 0 a 200
                if (randomNum>10): randomNum = randomNum-20       #Si num>100, buscamos el valor negativo
                #fila.append(1)
                fila.append(randomNum)                             #Nuestra matriz tendra valores de -100 a 100
            self.matriz.append(fila)

    def asignarValor(self, i, j, valor):
        self.matriz[i][j]=valor