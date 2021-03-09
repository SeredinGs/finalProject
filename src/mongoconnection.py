from pymongo import MongoClient, DESCENDING


def mongoconnector(user):
    client = MongoClient('192.168.1.30', 27017)
    db = client["course"]
    collection = db['prediction']
    a = list(collection.find({'user' : user}))
    if a:
        user = a[0]['user']
        pred = a[0]['prediction']
        return (user, pred)
    else:
        return (0000, 0)