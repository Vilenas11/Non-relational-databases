from pymongo import MongoClient
import pprint

 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
CONNECTION_STRING = "mongodb+srv://vilenas11:ievairvilius@cluster0.83eiaoa.mongodb.net/?retryWrites=true&w=majority"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
client = MongoClient(CONNECTION_STRING)
mydb = client["test"]
jobCollection = mydb["Jobs"]
employeeCollection = mydb["Employees"] 

#pipeline = [
#        {"$group": {"_id": "$job_id","Maximum_salary": {"$max": "$salary"}}},
#        {"$sort": {"Maximum_salary":1}}
#]
cursor = employeeCollection.aggregate([
        {"$group": {"_id": "$job_id","Maximum_salary": {"$max": "$salary"}}},
        {"$sort": {"Maximum_salary":1}}
])
#for document in cursor:
#   print(document)
#
#cursor1 = employeeCollection.find({},{"department_id": "$department_id", 
#"department_name": "$department_name","City": "$city"})
#for document1 in cursor1:
#   print(document1)

mapF =  """function() {
   emit(this.job_id, this.salary);
};"""
reduceF = """function(job_id, salary){
    var max=salary[0];
    salary.forEach(function(val){
        if (val > max) max=val;
    })
    return max;
}
"""

results = mydb.command(
   'mapreduce',
   'Employees',
   map = mapF,
   reduce = reduceF
)
print(results)