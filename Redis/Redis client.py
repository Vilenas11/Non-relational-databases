import redis


client = redis.Redis(host = 'localhost', port=6379)

def buying(r: redis.Redis, number1: int, number2: int):# -> None:
   with r.pipeline() as pipe:
        while True:
            try:
                pipe.watch(number1)
                pipe.watch(number2)
                stock = int(r.hget(number1, "Stock"))
                price = int(r.hget(number1, "Price"))
                money = int(r.hget(number2, "Money"))
                if stock > 0 and money >= price:
                   pipe.multi()
                   pipe.hincrby(number1, "Stock", -1)
                   pipe.hincrby(number1, "Sold", 1)
                   pipe.hincrby(number2, "Money", -price)
                   pipe.execute()
                   print(f"""""
                   ########################
                          !!!SOLD!!!
                   ########################
                   """)
                   break
                elif stock == 0 and money >= price:
                 #  Jei likuciu nepakanka, baigiam stebeti prekes raktus
                   pipe.unwatch()
                   print(f"""""
                   ########################################
                    !!! Sorry, {number1} out of stock! !!!
                   ########################################
                   """)
                   break
                elif stock > 0 and money < price :
                   pipe.unwatch()
                   print(f"""""
                   ########################################
                   !!! Sorry, insufficient cash balance !!!
                   ########################################
                   """)
                   break
                else:
                   pipe.unwatch()
                   print(f"""""
                   ##################################################################
                   !!! Sorry, insufficient cash and there are no items in stock !!!
                   ##################################################################
                   """)
                   break
            except redis.WatchError:
                print(f"""""
                   #################################################
                    !!! Sorry, someone else bought this product !!!
                   #################################################
                   """)  
                break
                #return soldOrNot


customer1 = client.hmget("Client1", "Name", "Surname", "Money")
#customer1 = client.hget("Client2", "Name")
customer2 = client.hmget("Client2", "Name", "Surname", "Money")


product1 = client.hmget("Product1", "Name", "Price", "Code","Balance")
product2 = client.hmget("Product2", "Name", "Price", "Code","Balance")
product3 = client.hmget("Product3", "Name", "Price", "Code","Balance")

#Spausdinam klientu sarasa
#print(f"""
#Customer list:
#
#Nr. 1: name, surname, balance : {customer1} 
#Nr. 2: name, surname, balance : {customer2}
#""")
print(f"""
Customer list:

Nr. 1: ||name: {client.hget("Client1", "Name")}   || surname:{client.hget("Client1", "Surname")}   ||Balance: {client.hget("Client1", "Money")} ||
Nr. 2: ||name: {client.hget("Client2", "Name")}     || surname:{client.hget("Client2", "Surname")}      ||Balance: {client.hget("Client2", "Money")} ||
""")
                        
#Spausdinam prekiu sarasa
#print(f"""
#Product list:
#
#Nr. 1: product name, price, product id, amount in stock :{product1}                   
#Nr. 2: product name, price, product id, amount in stock :{product2} 
#Nr. 3: product name, price, product id, amount in stock :{product3}
#""")                           

print(f"""
Product list:

Nr. 1: ||product name:  {client.hget("Product1", "Name")}        || price: {client.hget("Product1", "Price").ljust(5)}   || product id: {client.hget("Product1", "Name")}         || amount in stock: {client.hget("Product1", "Stock")} ||
Nr. 2: ||product name:  {client.hget("Product2", "Name")}       || price: {client.hget("Product2", "Price")}     || product id: {client.hget("Product2", "Name")}        || amount in stock: {client.hget("Product2", "Stock")} ||
Nr. 3: ||product name:  {client.hget("Product3", "Name")}  || price: {client.hget("Product3", "Price")}     || product id: {client.hget("Product3", "Name")}   || amount in stock: {client.hget("Product3", "Stock")} ||
#""")

#Funkcijos vygdymas, pirkejo ir prekes pasirinkimas
while True:
   try:
      cus = int(input("Enter customer No: 1 or 2 -> "))
   except ValueError:
      print("Wrong number. Enter customer No: 1 or 2 -> ")
      continue
   if cus < 1 or cus > 2:
      print("Wrong number. Enter customer No: 1 or 2 -> ")
      continue
   else:
      while True:
         try:
            go = int(input("Enter product No: 1, 2 or 3 -> "))
         except ValueError:
            print("Wrong number. Enter product No: 1, 2 or 3 -> ")
            continue
         if go < 1 or go > 3:
              print("Wrong number. Enter product No: 1, 2 or 3 -> ")
              continue
         else:
              #buying(client, f"Product{go}")
              buying(client, f"Product{go}", f"Client{cus}")
              break
      break
        
print(f"Customer {cus} cash balance:", client.hget(f"Client{cus}", "Money"))   
print(f"No. {go} stock:", client.hget(f"Product{go}", "Stock"))
print("Sold in total:", client.hget(f"Product{go}", "Sold"))
