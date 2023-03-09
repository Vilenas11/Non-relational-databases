import redis

client = redis.Redis(host = 'localhost', port=6379)

client.hmset("Client1", {
   "Name": "Vilius",
   "Surname": "Baranauskas",
   "Money": "1050",
   "Age": "21"
})

client.hmset("Client2", {
  "Name": "Ieva",
  "Surname": "Eigelyte",
  "Money": "2000",
  "Age": "18"
})

client.hmset("Product1", {
   "Name": "Car",
   "Code": "56235",
   "Price": 700,
   "Stock": 2,
   "Sold": 0
})
client.hmset("Product2", {
   "Name": "Bike",
   "Code": "64572",
   "Price": 300,
   "Stock": 1,
   "Sold": 0
})
client.hmset("Product3", {
   "Name": "Motorbike",
   "Code": "44778",
   "Price": 650,
   "Stock": 3,
   "Sold": 0
})
