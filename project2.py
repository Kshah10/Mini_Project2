import signal
import sys
from pymongo import MongoClient

databaseName = "291db"


# This function allows the user to exit the program at any time by hitting ctrl + c on their keyboard
# https://stackoverflow.com/questions/1112343/how-do-i-capture-sigint-in-python
def signal_handler(sig, frame):
    print("Exiting now ")
    sys.exit(0)


def article_search(dblp):
    #The user should be able to provide one or more keywords, and the system should retrieve all articles that match all those keywords (AND semantics). 
    #A keyword matches if it appears in any of title, authors, abstract, venue and year fields (the matches should be case-insensitive). 
    #For each matching article, display the id, the title, the year and the venue fields. 
    #The user should be able to select an article to see all fields including the abstract and the authors in addition to the fields shown before. 
    #If the article is referenced by other articles, the id, the title, and the year of those references should be also listed.
    print("You have chosen to search an article.")
    keyword_list = input("Please enter your keyword comma seperated keyword search: ")
    print('')
    words_list = keyword_list.split()
    slash_word_list = []
    for word in words_list:
        slash_word_list.append('\"')
        slash_word_list.append(word)
        slash_word_list.append('\"')
    use_list = ''.join(slash_word_list)

    print("This is what we found from the database: ")
    print('')
    c = dblp.find({"$text": {"$search": use_list, "$caseSensitive": False }}, {"id": 1, "title": 1, "year": 1, "venue": 1, "_id": 0})
    for i in c:
        print(i)
    print('')
    selection = input('Would you like to select an article? Y/N ')
    if selection == 'Y' or 'y':
        print('')
        article_id = input("Please enter the exact id of your selection: ")
        print('')
        curs = dblp.find_one({"id": article_id}, {"id": 1, "title": 1, "year": 1, "venue": 1, "abstract": 1, "authors": 1,  "_id": 0})
        print(curs)
        print('')
        print("This text is referenced by:")
        print('')
        ref = dblp.find({"References": article_id}, {"id": 1, "title": 1, "year": 1,"_id": 0})
        for i in ref:
            print(i)
        print('')
    else:
        return
    return

def author_search(dblp):
    #The user should be able to provide a keyword  and see all authors whose names contain the keyword 
    #(the matches should be case-insensitive). For each author, list the author name and the number of publications. 
    #The user should be able to select an author and see the title, year and venue of all articles by that author. 
    #The result should be sorted based on year with more recent articles shown first.
    print("You have chosen to search an author.")
    keyword= input("Please enter your keyword comma seperated author keyword search: ")
    print("This is what we found from the database: ")
    c = dblp.aggregate(
        [
            {
                "$match": {
                    "authors": {"$regex" : keyword, "$options" : "i"}
                    }
                }, 
                {
                    "$unwind": "$authors"
                        }, 
                        {
                "$match": {
                    "authors": {"$regex" : keyword, "$options" : "i"}
                    }
                }, 
        {
            "$group": {
                "_id": "$authors", "Publications": {"$sum": 1}
                }
                }
        ])
    for result in c:
        print(result)
    print('')
    a_select = input("Would you like to select an author to see more info about them? Y/N: ")
    if a_select == 'Y' or 'y':
        print('')
        author_name = input("Please enter the exact name of the author: ")
        print('')
        curs = dblp.find({"authors": author_name}, { "year": 1, "title": 1, "venue": 1,  "_id": 0}).sort("year", -1)
        for i in curs:
            print(i)
    return


def list_venue(dblp):
    print("You have chosen to see a listing of venues.")

    user_input = input("Please enter a number value to see the top listing of venue ")


    row = dblp.aggregate([   
    {
        "$group": {
            "_id": '$venue',
            "count": { "$sum": 1 },
            # "count2": {
            # "$sum": {
            #     "$size": {
            #         "ifNull":[ 
            #         {"$filter": {
            #             "input": "$references",
            #             "as": "el",
            #             "cond": {
            #                 "$eq": [ "$$el", "$id" ]
            #                 }
            #             }},
            #             []]
            #         }
            #     }
            # } 
        }
    },
    { "$sort" : { "count" : -1} },
    { "$limit" : int(user_input) }
    
    ]);
    



    # result = dblp.aggregate([
    # # {
    # #     "$unwind": "$references"

    # # },
    # { "$project": {
    #     "obj1": {
    #     "$filter": {
    #       "input": "$references",
    #       "as": "el",
    #       "cond": {
    #         "$and": [
    #           { "$eq": [ "$$el", "$id" ] }
    #         ]
    #       }
    #     }
    # }}},
    # # {"$match": { "eq": 1 } },
    # {
    #     "$group": {
    #         "_id": "$id"
    #     }
    # },
    # # { "$sort" : { "eq" : -1} },
    # { "$limit" : int(user_input) }
    # ])

    # # result2 =  dblp.aggregate([
    # #     {
    # #         "$lookup": {
    # #             "from": "dblp",
    # #             "localField": "id",
    # #             "foreignField": "references",
    # #             "as": "id_ref"
    # #         }
    # #     },
    # #     {
    # #         "$project": {
    # #             "id": "1",
    # #             "id_ref": {
    # #                 "$map": {
    # #                     "input": {
    # #                         "$filter": {
    # #                             "input": "$id_ref",
    # #                             "cond": {
    # #                                 "$eq": [
    # #                                     "$$this.references", "$id"
    # #                                 ]
    # #                             }
    # #                         }
    # #                     },
    # #                     "in": {

    # #                     }
    # #                 }
    # #             }
    # #         }
    # #     }

    # # ])
    # # for i in result2:
    # #     print(i)




    # for i in result:
    #     print(i)      

    for i in row:
        print(i)



    return

def add_article(dblp):
    print("You have chosen to add an article.")

    while(True):
        article_id = input('Please provide a valid unique id: ')
        if article_id.lower() == '':
            return



        # Make sure the provided ID is unique.
        unique = dblp.count_documents({"id":article_id})
        if unique != 0: #We want it to be equal to 0 but if it is equal to 1 then that means the id is in the collection already.
            print("Article id is not unique")
            continue
        break

    # Get other information from the user.

    title = input('Please provide a title: ').title() #.title() converts to a title like string. first letter becomes cap
    if title.lower() == '':
        return
    author_list = input('Please provide the list of authors: ')
    if author_list.lower() == '':
        return
    year = input('Please provide a year: ')
    if year.lower() == '':
        return

    #try adding into the collection
    try:
        dblp.insert_one({'abstract': 'NULL', 'authors': author_list, 'n_citation': 0, 'reference': {}, 'title': title, 'venue': 'NULL', 'year': year, 'id': article_id})
    except:
        print('An unknown error occurred.')
        return
    print('The article was successfully added')
    return


def task_selection():
    print("What would you like to do? Please make a numerical selection from the following:")
    print("1 Search for articles")
    print("2 Search for authors")
    print("3 List the venues")
    print("4 Add an article")
    print("5 Exit or press Ctrl + C")
    selectionList = ['1', '2', '3', '4', '5']
    validSelection = False
    while not validSelection:
        selection = input("Please make your numerical selection: ")
        if selection in selectionList:
            validSelection = True
            return selection
        else:
            print("That is not a valid selection, please try selecting again.")
    return


def main():
    portNumber = input("Please enter port number: ")
    clientPort = "mongodb://localhost:" + portNumber
    # Use client = MongoClient('mongodb://localhost:27017') for specific ports!
    client = MongoClient(clientPort)
    print(clientPort)

    # Create or open the 291db database on server.
    db = client["291db"]
    print("Connection was successful! ")

    # List collection names.
    collist = db.list_collection_names()
    if "dblp" in collist:
        print("The dblp collection exists.")

    # Create or open the collection in the db
    dblp = db["dblp"]

    while True:
        selection = task_selection()

        if selection == '1':
            article_search(dblp)
        # User wants to search for authors
        elif selection == '2':
            author_search(dblp)
        # User wants list the venues
        elif selection == '3':
            list_venue(dblp)
        # User wants to add an article
        elif selection == '4':
            add_article(dblp)
        # User wants to exit
        elif selection == '5':
            break
    return


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main()
