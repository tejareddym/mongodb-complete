#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pymongo import MongoClient

## aceess by using creating a object
spy = MongoClient()
##connect with port number and host
spy = MongoClient("mongodb://127.0.0.1:27017/")

mydb = spy["test_case"]

col = mydb["t_c"]

data = [{"user":"pant", "subject":"Database", "score":80}, 
    {"user":"cummins",  "subject":"JavaScript", "score":90}, 
    {"user":"rohit",  "title":"Database", "score":85}, 
    {"user":"cummins",  "title":"JavaScript", "score":75}, 
    {"user":"pant",  "title":"Data Science", "score":60},
    {"user":"pusocki",  "title":"Data Science", "score":95}]

col.insert_many(data)


# In[5]:


col.find_one()     ## defaulty displays first json file


# In[6]:


## select * from test_case("my sql")

##NOSQL
for r in col.find():
    print(r)


# In[11]:


for q in col.find({"subject":{"$in":["Data Science","Database"]}}):      ## the $in operator selects the documents where the value of the field equals any value in specified array
    print(q)


# In[12]:


## insert on json file by existing test_data

data ={"user":"sundar", "subject":"Data analytics", "score":90}

col.insert_one(data)


# In[15]:


data = {"user":"lakshman", "subject":".NET", "score":65}

col.insert_one(data)


# In[17]:


for x in col.find():
    print(x)


# In[21]:


## $gt operator :- selects those documents where the value of the field is greater than the specified value.

for x in col.find({"title":"Data Science","score":{"$gt":70}}):
    print(x)

$eq:-It will match the values that are equal to a specified values.
$gte:-It will match the values that are greater than or equal to a specified value.
$ne:-It will match all the values whch are not equal to a specified value.
$nin:-It will match none of the values specified in an array.
# In[22]:


## $or condition:-The $or operator performs a logical OR operation on an array of two or more <expressions> and selects the documents that satisfy at least one of the <expressions>.

for x in col.find({"$or":[{"user":"sundar"},{"title":"Data Science"}]}):
    print(x)


# In[ ]:





# updating records in mongodb
pymango.collection.Collection.update_one()
pymango.collection.Collection.update_many()
pymango.collection.Collection.replace_one()
# In[24]:


col.insert_many([
    {"item": "canvas",
     "qty": 100,
     "size": {"h": 28, "w": 35.5, "uom": "cm"},
     "status": "A"},
    {"item": "journal",
     "qty": 25,
     "size": {"h": 14, "w": 21, "uom": "cm"},
     "status": "A"},
    {"item": "mat",
     "qty": 85,
     "size": {"h": 27.9, "w": 35.5, "uom": "cm"},
     "status": "A"},
    {"item": "mousepad",
     "qty": 25,
     "size": {"h": 19, "w": 22.85, "uom": "cm"},
     "status": "P"},
    {"item": "notebook",
     "qty": 50,
     "size": {"h": 8.5, "w": 11, "uom": "in"},
     "status": "P"},
    {"item": "paper",
     "qty": 100,
     "size": {"h": 8.5, "w": 11, "uom": "in"},
     "status": "D"},
    {"item": "planner",
     "qty": 75,
     "size": {"h": 22.85, "w": 30, "uom": "cm"},
     "status": "D"},
    {"item": "postcard",
     "qty": 45,
     "size": {"h": 10, "w": 15.25, "uom": "cm"},
     "status": "A"},
    {"item": "sketchbook",
     "qty": 80,
     "size": {"h": 14, "w": 21, "uom": "cm"},
     "status": "A"},
    {"item": "sketch pad",
     "qty": 95,
     "size": {"h": 22.85, "w": 30.5, "uom": "cm"},
     "status": "A"}])


# In[25]:


## The $set operator replaces the value of a field with the specified value.
col.update_one({"item":"planner"},{"$set":{"size":"uom.mm","status":"X"},"$currentDate":{"lastModified":True}})


# In[28]:


col.update_many({"qty":{"$lte":60}},
                {"$set":{"size.uom":"km","status":"Y"},
                 "$currentDate":{"lastModified":True}})


# In[29]:


col.replace_one({"item":"sketch pad"},
               {"item":"sketch pad",
               "product":[{"type of wood":"Z","qty":123}]})


# ## Aggregate  and group  
1.AVG
2.SUM
3.GROUP
# In[4]:


## total num of records
agg_res = collection.aggregate([{"$group":
                                 {"_id":"$user"
                                "Total subject":{"$sum":1}}}])
for i in agg_res:
    print(i)


# In[8]:


agg_result= col.aggregate( 
    [{ 
    "$group" :  
        {"_id" : "$user",  
         "Total Subject" : {"$sum" : 1} 
         }} 
    ])
for i in agg_result:
    print(i)


# In[12]:


## $group put documents by the specified _id expression and for each distinct group.
agg_result= col.aggregate([{"$group" :
                            {"_id" : "$user",
                            "Total Marks" : {"$sum":"$score"}}
                            }])
for i in agg_result:
    print(i)


# In[13]:


## Returns the average value of the numeric values. $avg ignores non-numeric values.
agg_result= col.aggregate([{"$group" :
                            {"_id" : "$user",
                            "Average Score" : {"$avg":"$score"}}
                            }])
for i in agg_result :
    print(i)


# In[27]:


import datetime as datetime


# In[28]:


data=[{ "_id" : 1, "item" : "abc", "price" : 10, "quantity" : 2, "date" : datetime.datetime.utcnow()},
{ "_id" : 2, "item" : "jkl", "price" : 20, "quantity" : 1, "date" : datetime.datetime.utcnow() },
{ "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 5, "date" : datetime.datetime.utcnow() },
{ "_id" : 4, "item" : "abc", "price" : 10, "quantity" : 10, "date" : datetime.datetime.utcnow() },
{ "_id" : 5, "item" : "xyz", "price" : 5, "quantity" : 10, "date" :datetime.datetime.utcnow() }]


# In[29]:


data


# In[32]:


mycollection = mydb["stores"]
mycollection.insert_many(data) 


# In[31]:


agg_result = col.aggregate([{"$group":{"_id":"$item",
                                        "avg amount":{"$avg":{"$multiply":
                                                             ["$price","$quantity"]}},
                                        "avg quantity":{"$avg":"$quantity"}}}])
for i in agg_result:
    print(i)


# In[37]:



data = [ {"_id" : 1,
  "title": "abc123",
  "isbn": "0001122223334",
  "author": { "last": "zzz", "first": "aaa" },
  "copies": 5},
{
  "_id" : 2,
  "title": "Baked Goods",
  "isbn": "9999999999999",
  "author": { "last": "xyz", "first": "abc", "middle": "" },
  "copies": 2
},
{
  "_id" : 3,
  "title": "Ice Cream Cakes",
  "isbn": "8888888888888",
  "author": { "last": "xyz", "first": "abc", "middle": "mmm" },
  "copies": 5
}]


# In[40]:


col = mydb["Books"]


# In[41]:


col.insert_many(data)


# In[43]:


for row in col.aggregate([{"$project":{"title":1}}]) :      ##$project:- Used to select some specific fields from a collection.
    print(row)                                             


# In[ ]:




