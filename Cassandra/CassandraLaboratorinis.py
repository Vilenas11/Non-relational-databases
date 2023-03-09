from cassandra.cluster import Cluster
# service --status-all
# service start
# desc keyspaces;  // list all databases/collections names
# use anyKeyspace;  // select any database
# desc tables;      // list all tables in collection/ database
# https://docs.datastax.com/en/developer/python-driver/3.23/getting_started/
# cqlsh

# CREATE KEYSPACE − Creates a KeySpace in Cassandra.
# USE − Connects to a created KeySpace.
# ALTER KEYSPACE − Changes the properties of a KeySpace.
# DROP KEYSPACE − Removes a KeySpace
# CREATE TABLE − Creates a table in a KeySpace.
# ALTER TABLE − Modifies the column properties of a table.
# DROP TABLE − Removes a table.
# TRUNCATE − Removes all the data from a table.
# CREATE INDEX − Defines a new index on a single column of a table.
# DROP INDEX − Deletes a named index.

# INSERT − Adds columns for a row in a table.
# UPDATE − Updates a column of a row.
# DELETE − Deletes data from a table.
# BATCH − Executes multiple DML statements at once.
# SELECT − This clause reads data from a table
# WHERE − The where clause is used along with select to read a specific data.
# ORDERBY − The orderby clause is used along with select to read a specific data in a specific order.
# $ nodetool status 
# nodetool //-h hostname -p port\\ ring //nodes info
def printRows():
  print("""
  ////////////////////////////////////////
  //////////////////TOWN//////////////////
  ////////////////////////////////////////
  """)
  townRows = session.execute("SELECT * FROM database1.town;")
  for town_row in townRows:
    print(town_row)
  print("""
  /////////////////////////////////////////////
  //////////////////BUILDINGS//////////////////
  /////////////////////////////////////////////
  """)
  buildingRows = session.execute("SELECT * FROM database1.building;")
  for building_row in buildingRows:
    print(building_row)
  print("""
  ///////////////////////////////////////////
  //////////////////CITIZEN//////////////////
  ///////////////////////////////////////////
  """)
  citizenRows = session.execute("SELECT * FROM database1.citizen;")
  for citizen_row in citizenRows:
    print(citizen_row)

#def printQuery():
  

cluster = Cluster(['0.0.0.0'], port=9042)
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS database1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
session = cluster.connect("database1")

session.execute("""CREATE TABLE IF NOT EXISTS database1.town (
  name text,
  population bigint,
  size bigint,
  zipcode smallint,
  country text,
  PRIMARY KEY (zipcode, name)
);""")
session.execute("""
CREATE TABLE IF NOT EXISTS database1.citizen(
  id int,
  name text,
  age smallint,
  zipcode smallint,
  PRIMARY KEY(zipcode, id)
)

""")
session.execute("""
CREATE TABLE IF NOT EXISTS database1.building(
  zipcode smallint,
  house_nr int,
  street text,
  type text,
  PRIMARY KEY((house_nr, street), zipcode)
)
 
""")
#///////////////////////////INSERTING TOWN DATA\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(221, 'Lithuania','Anciunai',100, 5677) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(222, 'Poland','Warsaw',2555, 3242341) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(223, 'Lithuania','Sirvintos',1231, 31215665) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(224, 'Slovakia','Presov',45665, 213123213) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(225, 'USA','Dalas',123, 9787843) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(226, 'Slovakia','Bardejov',3543, 897534) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(227, 'USA','Boston',1111, 678435) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(228, 'Poland','Gdansk',18762, 234234) IF NOT EXISTS;")
session.execute("INSERT INTO database1.town (zipcode,country,name,population,size) VALUES(229, 'Ukraine','Kharkov',4232345, 765456) IF NOT EXISTS;")

#//////////////////////INSERTING BUILDING DATA\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(1, 'Ziedo', 221,'Flat') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(2, 'Miesto', 223,'House') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(3, 'Tulpiu', 222,'Flat') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(4, 'Sodo', 229,'Flat') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(5, 'Vinco', 227,'House') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(6, 'Plento', 224,'Hotel') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(7, 'Ziedo', 221,'Hotel') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(8, 'Meiles', 228,'Dorm') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(9, 'Saules', 227,'Flat') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(10, 'Tulpiu', 222,'Hotel') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(11, 'Aistros', 221,'Dorm') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(12, 'Didlaukio', 226,'House') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(13, 'Naugarduko', 226,'House') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(14, 'Pylimo', 229,'Hotel') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(15, 'Didzioji', 223,'Office') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(16, 'Ozo', 224,'DOrm') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(17, 'Bernardinu', 225,'Dorm') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(18, 'Bernardinu', 225,'Office') IF NOT EXISTS;")
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(19, 'Maironio', 226,'Flat') IF NOT EXISTS;")

#//////////////////////////INSERTING CITIZEN DATA\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(222, 1, 21, 'Vilius') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(226, 2, 21, 'Ieva') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(227, 3, 19, 'Karolis') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(221, 4, 20, 'Karina') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(223, 5, 30, 'Fausta') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(229, 6, 22, 'Evelina') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(228, 7, 21, 'Domantas') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(224, 8, 30, 'Ineta') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(225, 9, 27, 'Petras') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(221, 10, 21, 'Laimis') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(229, 11, 15, 'Eimantas') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(229, 12, 19, 'Gerda') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(222, 13, 22, 'Ramunas') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(229, 14, 30, 'ALdona') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(227, 15, 32, 'Rita') IF NOT EXISTS;")
session.execute("INSERT INTO database1.citizen (zipcode,id,age,name) VALUES(226, 16, 22, 'Gidrius') IF NOT EXISTS;")
##rows = session.execute('SELECT * FROM learn_cassandra.users_by_country;')
#for employee_row in rows:
#    print(employee_row)
#session.execute('describe tables;')
#cqlsh
#def search_nr2():
#
#    name = input("Iveskite vartotojo varda (username): ")
#    rows = session.execute(
#        f"select * from music_library.users_albums where username='{name}'"
#    )
#    if not rows:
#        print("Tokio vartotojo nera")
#    else:
#        for row in rows:
#            print("Rezultatai:", row[::])
#    album_name = input("Iveskite albumo pavadinima: ")
#    rows = session.execute(
#        f"select * from music_library.users_albums where username='{name}' and album_name='{album_name}'"
#    )
#    if not rows:
#        print("Tokio albumo nera")
#    else:
#        for row in rows:
#            print("Rezultatai:", row[::])
session.execute("INSERT INTO database1.building (house_nr,street,zipcode,type) VALUES(30, 'Antakalnio', 230,'Flat') IF NOT EXISTS;")
session.execute("UPDATE database1.building SET type = 'Flat' WHERE house_nr = 12 AND street = 'Didlaukio' AND zipcode = 226 IF type = 'House'")



def find1():
    lines = session.execute("Select * from database1.building where street = 'Didlaukio' and house_nr = 12")
    if not lines:
        print("Not found")
    else:
        for line in lines:
            print("Results:", line[::]) 


find2 = session.execute("Select count(*) from database1.building")
for find2 in find2:
    print("Count:  ", find2)

def find3():
    lines = session.execute(
        f"select * from database1.citizen where zipcode = 222")
    if not lines:
        print("Not found")
    else:
        for line in lines:
            print("Results:", line[::])

print(find1())
print(find3())
print(" DONE ")
#printQuery()