import sys
from pyspark import SparkContext
sc = SparkContext()

def mapper(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2
        
"""Dada una tupla que consta de un nodo y la lista con sus nodos adyacentes, devuelve las conexiones derivadas por el nodo presentes en el grafo"""
def conexiones(tupla):
    lista_conexiones = []
    for i in range(len(tupla[1])): #Para cada elemento de la lista de adjacencia 
        lista_conexiones.append(((tupla[0],tupla[1][i]),'exists')) #Añadimos la relacion directa con el principal
        for j in range(i+1,len(tupla[1])): #Todos los demas de la lista estan relacionados con el nodo i-esimo a traves del principal
            if tupla[1][i] >= tupla[1][j]: #Añadimos dicha relacion 
                lista_conexiones.append(((tupla[1][j],tupla[1][i]),('pending',tupla[0])))
            else:
                lista_conexiones.append(((tupla[1][i],tupla[1][j]),('pending',tupla[0])))
    return lista_conexiones

"""Dada una tupla cuya primera componente presenta dos nodos y su segunda contiene una lista con todas las relaciones entre ellas """
def posible_triciclo(tupla): #Para que pueda formar un 3-ciclo tiene que tener como minimo dos relaciones y una de ellas ser directa
    return ('exists' in tupla[1] and len(tupla[1])>= 2)

"""Dada una tupla cuya primera componente presenta dos nodos y su segunda contiene una lista con todas las relaciones entre ellas 
previamente abremos aplicado la funcion 'posible_triciclo' luego sabemos que existe una relacion directa y hay mas relaciones 
por tanto toda relacion indirecta que haya entre los dos nodos a traves de un tercero pasa a formar un triciclo que añadimos
"""
def triciclos(tupla): 
    triciclo = []
    for relacion in tupla[1]:
        if relacion != 'exists':
            triciclo.append((relacion[1],tupla[0][0], tupla[0][1]))
    return triciclo

def ejercicio_uno(sc,filename):
    graph_clean = sc.textFile(filename).map(mapper).filter(lambda x: x != None).distinct()
    adjacents = graph_clean.groupByKey() #RDD de tuplas donde a cada nodo le acompaña su RDD de nodos adyacentes, forma ('A', ['B','C','D','F']) 
    rdd_conexiones = adjacents.mapValues(list).flatMap(conexiones)  #RDD con todas las conexiones presentes en el grafo de la forma ((A, B) , exists) o bien ((B, C), ('pending', A))                   
    rdd_triciclos = rdd_conexiones.groupByKey().mapValues(list).filter(posible_triciclo).flatMap(triciclos) #RDD con todos los triciclos del grafo
    print(rdd_triciclos.collect())
    return rdd_triciclos.collect()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {0} <file>")
    else:
        ejercicio_uno(sc,sys.argv[1])

