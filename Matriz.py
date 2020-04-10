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
    
    def getMatriz(self):
        return self.matriz

    def getnFilas(self):
        return self.nFilas
   
    def getnColumns(self):
        return self.nColumns

    def asignarValor(self, i, j, valor):
        self.matriz[i][j]=valor

    def comprobarValor(self, i, j):
        return self.matriz[i][j]

    def multiplicacionMatrices(self, m):
        if (self.nColumns!=m.getnFilas()): raise Exception("Imposible de realizar la multiplicacion")
        resultado = Matriz(self.nFilas,m.getnColumns())
        for i in range(self.nFilas):
            for j in range(m.getnColumns()):
                suma=0
                for c in range(self.nColumns):
                    suma+=self.matriz[i][c]*(m.getMatriz())[c][j]
                resultado.asignarValor(i,j,suma)
        return resultado