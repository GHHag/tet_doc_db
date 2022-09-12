import pickle

from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection


class TetMlModelsDb:

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__systems: Collection = self.__client.systems
        self.__ml_models: Collection = self.__client.ml_models
    
    def _insert_system(self, system_name):
        self.__systems.insert_one({'name': system_name})
    
    def _get_system_id(self, system_name):
        query = self.__systems.find_one(
            {'name': system_name},
            {'_id': 1}
        )
        return query['_id'] if query else None

    def insert_ml_model(self, system_name, instrument, model):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        self.__ml_models.update_one(
            {'system_id': system_id, 'system_name': system_name, 'instrument': instrument},
            {'$set': {'model': model}}, upsert=True
        )
        return True

    def get_ml_model(self, system_name, instrument):
        system_id = self._get_system_id(system_name)
        query = self.__ml_models.find_one(
            {'system_id': system_id, 'system_name': system_name, 'instrument': instrument}, 
            {'_id': 0, 'model': 1}
        )
        return pickle.loads(query['model'])
