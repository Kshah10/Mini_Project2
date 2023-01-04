from pprint import pprint

import pymongo
import json
from pymongo import MongoClient, InsertOne, TEXT
import os

import argparse
from bson import json_util


def main():
    portNumber = input("Please enter port number: ")
    clientPort = "mongodb://localhost:" + portNumber

    client = MongoClient(clientPort)
    print(clientPort)

    jsonFileName = input("Please enter json file name: ")
    # Create or open the 291db database on server.
    db = client["291db"]

    collist = db.list_collection_names()
    if "dblp" in collist:
        # Delete collection if it exists
        print("Removing existing collection dblp now.")
        dblp = db["dblp"]
        dblp.drop()

    dblp = db["dblp"]
    # https: // www.mongodb.com / compatibility / json - to - mongodb
    requesting = []

    # works for large import
    # https://www.mongodb.com/docs/database-tools/mongoimport/
    command = "mongoimport --host localhost:" + portNumber + " --db 291db --collection dblp --file " + jsonFileName
    # Also add --batchsize = 1
    os.system(command)


    # for 1st query
    dblp.create_index([('title', 'text'), ('authors', 'text'), ('abstract', 'text'),
                       ('venue', 'text'), ('year', 'text')])


    #print(dblp.index_information())

    client.close()
    return


if __name__ == "__main__":
    main()
