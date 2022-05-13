from pyspark import SparkContext
import sys


def get_edges(line): # grafo no dirigido, eliminamos bucles.
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2
        
        
def possible_nodes(l): # función auxiliar para map
    res = []
    for i in range(0,len(l)):
        for j in range(i+1, len(l)):
            res.append((l[i],l[j]))
    return res
    

def create_cycles(adj): # función auxiliar para map
    res = []
    for i in range(len(adj[1])):
        res.append((adj[0], adj[1][i][0], adj[1][i][1]))
    return res

def triciclos(sc, filename):
    edges = sc.textFile(filename).\
        map(get_edges).\
        filter(lambda x: x is not None).\
        distinct()
    edges_l = edges.collect()
    adj = edges.groupByKey()
    possible = adj.map(lambda x: (x[0], possible_nodes(sorted(list(x[1]))))).\
        filter(lambda x: x[1] != [])
    triciclos = possible.flatMap(create_cycles).\
    	filter(lambda x : x[1:3] in edges_l)
    return triciclos.collect()

def main(sc, filename):
	print ('RESULTS------------------')
	result = triciclos(sc, filename)
	print(result)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
