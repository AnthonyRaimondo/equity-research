import os
from typing import Union, Any

from pymongo import MongoClient, results, database


def write(
    db_name: str, collection_name: str, document: Any, insert_one: bool = False
) -> Union[results.InsertManyResult, results.InsertOneResult]:
    client: MongoClient
    return_obj: Union[results.InsertManyResult, results.InsertOneResult]
    with MongoClient(host=os.environ.get("HOST"), port=int(os.environ.get("PORT"))) as client:

        db: database.Database = client[db_name]
        collection = db[collection_name]

        if insert_one:
            return_obj = collection.insert_one(document)
        else:
            return_obj = collection.insert_many(document)

    return return_obj


def read(db_name: str, collection_name: str, query: dict) -> dict:
    client: MongoClient
    with MongoClient(host=os.environ.get("HOST"), port=int(os.environ.get("PORT"))) as client:

        _db: database.Database = client[db_name]
        _collection = _db[collection_name]

        document = _collection.find_one(query)

    return document


def get_unique_ids(db_name: str, collection_name: str, query: str) -> list:
    client: MongoClient
    with MongoClient(host=os.environ.get("HOST"), port=int(os.environ.get("PORT"))) as client:

        _db: database.Database = client[db_name]
        _collection = _db[collection_name]

        unique_ids = _collection.distinct(query)

    return unique_ids
