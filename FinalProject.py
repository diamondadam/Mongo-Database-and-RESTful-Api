import json
from bson import json_util
from bson.son import SON
from pymongo import MongoClient
from pymongo import errors
from pprint import pprint
import bottle
from bottle import route, post, run, request, abort, get, put, delete

#Create connections for the correct database and collection
connection = MongoClient('localhost', 27017)
db = connection['market']
collection = db["stocks"]

#Function for the aggregation pipline
def aggregation():
  #Find by sector
  #Return  total outstanding shares grouped by document key industry
  value = raw_input("Please enter the Sector: ")
  #Create aggregate pipline
  pipeline = [
    {"$match":{"Sector": value}},
    {"$group" : {"_id" : "$Industry", "Outstanding shares" : {"$sum": "$Shares Outstanding"}}}
  ]
  try:
    result = list(collection.aggregate(pipeline))
    pprint(result)
  except Exception as e:
     print("Aggregation error: ", e)

#Simple findone
def findOne():
  try:
    result = collection.find_one()
    pprint(result)
  except Exception as e:
      print("Aggregation error: ", e)

def find_Low_High():
  low = raw_input("Please enter the low 50-Day Simple Moving Average value: ")
  high = raw_input("Please enter the high 50-Day Simple Moving Average value: ")
  try:
    result = collection.find({"$and":[{"50-Day Simple Moving Average":{"$gt": int(low)}}, {"50-Day Simple Moving Average":{"$lt": int(high)}}]})
    print("Count is: ", result.count())
  except Exception as e:
     print("Find error: ", e)

def find_industry():
  value = raw_input("Please enter the Industry: ")
  try:
    for x in collection.find({"Industry" : value}, {"Ticker": 1}):
      print x
  except Exception as e:
     print("Find error: ", e)

#Inserts one document by query criteria
def insert_document(document):
  try:
    doc = json.loads(document)
    print(doc)
    result = collection.insert_one(doc)
    print(result)
    print(True)
  except Exception as e:
    print(False)
    print("Insert error: ", e)

#Finds documents by query criteria
def find_document(query):
  try:
    for x in collection.find(query):
      print x
  except Exception as e:
     print("Find error: ", e)

#Updates one document by query criteria
def update_document():
  kValue = raw_input("Please enter the Ticker value: ")
  uValue = raw_input("Please enter the new Volume value: ")
  sKey = "Ticker"
  uKey = "Volume"
  query = {sKey : kValue}
  newValue = { "$set": {uKey : uValue} }
  if int(uValue) <= 0:
    print("\n***Volume value must be greater than 0***\n")
    update_document()
  else:
    try:
      result = collection.update_one(query, newValue)
      print(result)
    except Exception as e:
      print("Update Error", e)

#Deletes one document by query criteria
def delete_document(query):
  try:
    result = collection.find_one_and_delete(query)
    print(result)
  except Exception as e:
    print("Delete error: ", e)

#Function called when the route /industryReport/<Industry> is used
@route('/industryReport/<Industry>', method = 'GET')
def industry_report(Industry):
  try:
    print(Industry)
    pipeline = [
    {"$match":{"Industry": Industry}},
    {"$sort" : {"Profit Margin" : 1}},
    {"$project" : {"Ticker" : 1}},
    {"$limit" : 5}]
    result = list(collection.aggregate(pipeline))
    pprint(result)
  except Exception as e:
     print("Stock Report error: ", e)

#Function called when the route stockReport is used
@route('/stockReport', method = 'POST')
def stock_report():
  try:
    all_selected = request.json.get('Ticker')
    for val in all_selected:
      query = {"Ticker" : val}
      find_stock_report(query)
  except Exception as e:
     print("Stock Report error: ", e)

#Function called when the route createStock is used
@post('/createStock')
def insert_doc():
  try:
    identifier = request.forms.get('Ticker')
    print(identifier)
    document = {"Ticker": identifier}
    result = collection.insert_one(document)
    query = {"Ticker" : identifier}
    find_doc(query)
  except Exception as e:
     print("Insert error: ", e)

#Function called to iterate of a collection for stock reports
def find_stock_report(query):
  try:
    for x in collection.find(query):
      pprint(x)
  except Exception as e:
     print("Find error: ", e)

#Function called to iterate of a collection documents
def find_doc(query):
  try:
    for x in collection.find(query):
      print x
  except Exception as e:
     print("Find error: ", e)

#Function called when the route readStock is used
@route('/readStock/<ticker>', method = 'GET')
def read_doc(ticker):
  try:
    string = ticker
    query = {"Ticker" : string}
    #Calls a seperaate function to find doc
    find_doc(query)

  except Exception as e:
     print("Read error: ", e)

#Function called when the route updateStock is used
@route('/updateStock', method='PUT')
def update_doc():
  try:
    param1 = request.forms.get('Ticker')
    param2 = request.forms.get('toUpdate')
    param3 = request.forms.get('Val')
    print(param1)
    print(param2)
    print(param3)
    query = {"Ticker" : param1}
    newValue = { "$set": {param2 : param3} }
    result = collection.update_one(query, newValue, upsert = True)
    find_doc(query)

  except Exception as e:
     print("Read error: ", e)

#Function called when the route deleteStock is used
@delete('/deleteStock')
def delete_doc():
  try:
    result = request.forms.get("Ticker")
    query = {"Ticker" : result}
    result = collection.find_one_and_delete(query)
    print(result)
  except Exception as e:
     print("Read error: ", e)


def main():
  #intitialize variables
  answer = 10

  #prints the selection menu
  print
  print("Welcome, please select from the following options:")
  print
  print("1. Create a document in market db and stocks collection")
  print("2. Read a document in market db and stocks collection")
  print("3. Update the Volume document > 0 in market db and stocks collection")
  print("4. Delete a document in market db and stocks collection")
  print("5. Find documents for which the 50-Day Simple Moving Average is between the low and high values")
  print("6. Find documents by Industry")
  print("7. Find One")
  print("8. Aggregation")
  print("9. Activate server")
  print("0. Press 0 to exit")
  #while loop to handle the menu input
  while answer != 0:
    try:
      answer = int(input("Awaiting instructions..."))
    except:
      print("Input error...")
    #Calls the insert method
    if answer == 1:
      key = raw_input("Please enter the document JSON key:value pair: ")
      document = key
      insert_document(document)
    #Calls the find method
    if answer == 2:
      key = "Ticker"
      value = raw_input("Please enter the Ticker value to find: ")
      query = {key : value}
      find_document(query)

    #Calls the update method
    if answer == 3:
      update_document()
    #Calls the delete method
    if answer == 4:
      key = "Ticker"
      value = raw_input("Please enter the Ticker value to be deleted: ")
      query = {key : value}
      delete_document(query)
    if answer == 5:
      find_Low_High()
    if answer == 6:
      find_industry()
    if answer == 7:
      findOne()
    if answer == 8:
      aggregation()
    if answer == 9:
      if __name__ == '__main__':
        #app.run(debug=true)
        run(host='localhost', port= 8080)
main()
