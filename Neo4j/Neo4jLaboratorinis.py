#!/usr/bin/env python
# coding: utf-8

# In[31]:


#!pip install neo4j
#!pip install py2neo

from neo4j import GraphDatabase
from py2neo import Graph
from py2neo.data import Node, Relationship
from py2neo.ogm import *
import pandas as pd

# Connect to the neo4j database server
graphdb = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j","password"))

session = graphdb.session()

# Query to delete all graph
deletequery = "MATCH (n) DETACH DELETE n"

createquery = """ CREATE (Sirvintos:Town {name: "Sirvintos", population: 5000}),
(Kaunas:Town {name: "Kaunas", population: 301300}),
(Vilnius:Town {name: "Vilnius", population: 552787}),
(Klaipeda:Town {name: "Klaipeda", population: 15288}),
(Siauliai:Town {name: "Siauliai", population: 101151}),
(Panevezys:Town {name: "Panevezys", population: 85885}),
(Ukmerge:Town {name: "Ukmerge", population: 20154}),
(Varena:Town {name: "Varena", population: 7892}),
(Anyksciai:Town {name: "Anyksciai", population: 10575}),
 
(Sirvintos)-[:Connected_to {distance: 109}]-> (Kaunas),
(Sirvintos)-[:Connected_to {distance: 51}]-> (Vilnius),
(Sirvintos)-[:Connected_to {distance: 313 }]-> (Klaipeda),
(Sirvintos)-[:Connected_to {distance: 92}]->(Panevezys),
(Sirvintos)-[:Connected_to {distance: 28}]->(Ukmerge),
(Sirvintos)-[:Connected_to {distance: 95}]->(Varena),
(Sirvintos)-[:Connected_to {distance: 67}]->(Anyksciai),


(Vilnius)-[:Connected_to {distance: 103}]->(Kaunas),
(Vilnius)-[:Connected_to {distance: 306}]->(Klaipeda),
(Vilnius)-[:Connected_to {distance: 213}]->(Siauliai),
(Vilnius)-[:Connected_to {distance: 137}]->(Panevezys),
(Vilnius)-[:Connected_to {distance: 73}]->(Ukmerge),
(Vilnius)-[:Connected_to {distance: 83}]->(Varena),
(Vilnius)-[:Connected_to {distance: 112}]->(Anyksciai),

(Kaunas)-[:Connected_to {distance: 214}]->(Klaipeda),
(Kaunas)-[:Connected_to {distance: 177}]->(Siauliai),
(Kaunas)-[:Connected_to {distance: 109}]->(Panevezys),
(Kaunas)-[:Connected_to {distance: 70}]->(Ukmerge),
(Kaunas)-[:Connected_to {distance: 128}]->(Varena),
(Kaunas)-[:Connected_to {distance: 112}]->(Anyksciai),

(Klaipeda)-[:Connected_to {distance: 173}]->(Siauliai),
(Klaipeda)-[:Connected_to {distance: 241}]->(Panevezys),
(Klaipeda)-[:Connected_to {distance: 278}]->(Ukmerge),
(Klaipeda)-[:Connected_to {distance: 332}]->(Varena),
(Klaipeda)-[:Connected_to {distance: 304}]->(Anyksciai),

(Siauliai)-[:Connected_to {distance: 96}]->(Panevezys),
(Siauliai)-[:Connected_to {distance: 144}]->(Ukmerge),
(Siauliai)-[:Connected_to {distance: 266}]->(Varena),
(Siauliai)-[:Connected_to {distance: 154}]->(Anyksciai),

(Panevezys)-[:Connected_to {distance: 69}]->(Ukmerge),
(Panevezys)-[:Connected_to {distance: 215}]->(Varena),
(Panevezys)-[:Connected_to {distance: 61}]->(Anyksciai),

(Ukmerge)-[:Connected_to {distance: 150}]->(Varena),
(Ukmerge)-[:Connected_to {distance: 45}]->(Anyksciai),

(Varena)-[:Connected_to {distance: 189}]->(Anyksciai)"""


# Creating graph
with graphdb.session() as graphdb_session:
    
    # Delete all
    nodes=graphdb_session.run(deletequery)
    
    # Create nodes
    graphdb_session.run(createquery)
    
    print("!!!CREATED!!!")
    
    
# 2.1
# Find cities where population ir more than n

def populationquery(citizens: int):
    citypop = f""" MATCH (n:Town) WHERE n.population > {citizens}
    RETURN n.name, n.population"""

    with graphdb.session() as graphDB_Session:
        population = graphDB_Session.run(citypop)
        
        print(f"Population more than {citizens}: \n")
        
        for record in population:
            print(f'City_name: {record["n.name"]} - population:{record["n.population"]}')

# 2.2
# Find all direct path from X

def allpaths(name: str):
    from_x = """ MATCH p=(from:Town {name: $name})-[:Connected_to]->()
UNWIND nodes(p) as node
WITH p, collect(node.name) as names
RETURN names"""

    with graphdb.session() as graphDB_Session:

        queryall = graphDB_Session.run(from_x,name=name)
    
        print(f"All path direct from {name}: \n")

        for record in queryall:
            print(record["names"])
            
# 2.3
# All path to go from X to Y

def fromxtoy(fromcity,tocity):
    from_x_to_y = """ MATCH p=(from:Town {name: $fromcity})-[:Connected_to*]->(to:Town {name: $tocity})
UNWIND nodes(p) as node
WITH p, collect(node.name) as names
RETURN names"""

    with graphdb.session() as graphDB_Session:

        record = graphDB_Session.run(from_x_to_y,fromcity=fromcity,tocity=tocity)
    
        print(f"All path from {fromcity} to {tocity}: \n")

        for record in record:
            print(record["names"])
            
            
# 2.4
# Shortestpath from X to Y
def shortest(fromcity, tocity):
    ShortestPath = """ MATCH (from:Town {name: $fromcity}),
    (to:Town {name: $tocity}) ,
    path = shortestPath((from)-[:Connected_to*]->(to))
    RETURN path"""

    with graphdb.session() as graphDB_Session:
    # Find the shortest path between two nodes

        shortestPath = graphDB_Session.run(ShortestPath,fromcity = fromcity,tocity=tocity)
    
        print(f"Shortest path between nodes - {fromcity} and {tocity}: \n")

        for record in shortestPath:
            nodes = record["path"].nodes
            for record in nodes:
                 print( record["name"])
            

# 2.5
# Aggregation to find total distance from X to Y

def aggregation(fromcity,tocity):
    aggreg = """ MATCH (from:Town {name:"Sirvintos"}), (to:Town {name: "Siauliai"}), 
path = (from)-[:Connected_to*]->(to)
UNWIND nodes(path) as node
RETURN  from.name, to.name, count(node) as count,
reduce (distance = 0, r in relationships(path) | distance+r.distance) AS totalDistance
ORDER BY totalDistance ASC
"""

    with graphdb.session() as graphDB_Session:
        record = graphDB_Session.run(aggreg,fromcity=fromcity,tocity=tocity)
        print(f"Total distance from {fromcity} to {tocity} and count of visited nodes:\n")
        for record in record:
            print(f'{record["from.name"]} -> {record["to.name"]} you will visit {record["count"]} citys and total distance - {record["totalDistance"]}')
        

        
print(f"""In graph we have 9 cities, distance from cities and cities population. 
              Please choose which query you want to execute
              
Cities list: 
1. Sirvintos   2. Kaunas
3. Vilnius     4. Klaipeda
5. Siauliai    6. Panevezys
7. Ukmerge     8. Varena
9. Anyksciai \n""")

def checkCity(input):
    if (input == 'Sirvintos') or (input == 'Kaunas') or (input == 'Vilnius') or (input == 'Klaipeda') or (input == 'Siauliai') or (input == 'Panevezys') or (input == 'Ukmerge') or (input == 'Varena') or (input == 'Anyksciai'):
       return True
    else:
        return False




def startWorking():
    while True:
        inputas = input("1: Find population,\n2: All paths from X,\n3: All path from X to Y,\n4: Shortest Path,\n5: Agregation, \n0: END WORK \n")

        if inputas == '1':
            citizens = input("Input minimum population criterion: ")
            populationquery(citizens)
        
        elif inputas == '2': 
            name = input("Input city name: ")
            if checkCity(name) == True:
                allpaths(name)
            else:
                 print("\nBad input!!!!!!!! Try again!!!!!!!!!!!\n")
        elif inputas == '3':
            fromcity = input("From: ")
            tocity = input("Going to: ")
            if checkCity(fromcity) == True and checkCity(tocity) == True:
                fromxtoy(fromcity,tocity)
            else: 
                print("\nBad input!!!!!!!! Try again!!!!!!!!!!!\n")
            
        elif inputas == '4':
            fromcity = input("From: ")
            tocity = input("Going to: ")
            if checkCity(fromcity) == True and checkCity(tocity) == True:
                shortest(fromcity,tocity)
            else: 
                print("\nBad input!!!!!!!! Try again!!!!!!!!!!!\n")
        
        elif inputas == '5':
            fromcity = input("From:")
            tocity = input("Going to:")
            if checkCity(fromcity) == True and checkCity(tocity) == True:
                aggregation(fromcity,tocity)
            else: 
                print("\nBad input!!!!!!!! Try again!!!!!!!!!!!\n")

            
        elif inputas == '0':
            break

        else: 
            print(f"\n !!! Invalid number !!!")
            startWorking()
            
    
startWorking()


# In[ ]:




