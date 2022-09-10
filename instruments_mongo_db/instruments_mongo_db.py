import json

from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from bson import json_util, objectid


class InstrumentsMongoDb:

    MARKET_LISTS_COLLECTION = 'market_lists'
    INSTRUMENTS_COLLECTION = 'instruments'

    ID_FIELD = '_id'
    MARKET_LIST_FIELD = 'market_list'
    MARKET_LIST_IDS_FIELD = 'market_list_ids'
    SYMBOL_FIELD = 'symbol'

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__market_lists: Collection = self.__client[self.MARKET_LISTS_COLLECTION]
        self.__instruments: Collection = self.__client[self.INSTRUMENTS_COLLECTION]

    def get_market_list_id(self, market_list_name):
        query = self.__market_lists.find_one(
            {self.MARKET_LIST_FIELD: market_list_name},
            {self.ID_FIELD: 1}
        )
        return query[self.ID_FIELD]

    def get_market_lists(self):
        return json.dumps(
            list(self.__market_lists.find()), default=json_util.default
        )

    def get_market_list_by_id(self, market_list_id):
        market_list_id = objectid.ObjectId(market_list_id)
        return json.dumps(
            self.__market_lists.find_one({self.ID_FIELD: market_list_id}), 
            default=json_util.default
        )

    def get_market_list_by_name(self, market_list_name):
        return json.dumps(
            self.__market_lists.find_one({self.MARKET_LIST_FIELD: market_list_name}),
            default=json_util.default
        )

    def get_market_list_instruments(self, market_list_id):
        query = self.__market_lists.aggregate(
            [
                {
                    '$match': {
                        self.ID_FIELD: objectid.ObjectId(market_list_id)
                    }
                },
                {
                    '$lookup': {
                        'from': self.INSTRUMENTS_COLLECTION,
                        'localField': self.ID_FIELD,
                        'foreignField': self.MARKET_LIST_IDS_FIELD,
                        'as': 'market_list_instruments'
                    }
                },
                {
                    '$project': {
                        f'market_list_instruments.{self.ID_FIELD}': 1,
                        f'market_list_instruments.{self.SYMBOL_FIELD}': 1
                    },
                }
            ]
        )
        return json.dumps(list(query)[0], default=json_util.default)

    def get_market_list_instrument_symbols(self, market_list_id):
        market_list_instruments = json.loads(
            self.get_market_list_instruments(market_list_id)
        )
        return json.dumps(
            [i['symbol'] for i in market_list_instruments['market_list_instruments']]
        )

    """ OMXS_COLLECTION_STRING = 'omxs_instruments'
    OMXS30_COLLECTION_STRING = 'omxs30_instruments'
    OMXSPI_COLLECTION_STRING = 'omxspi_instruments'
    OMXS_LARGE_CAPS_COLLECTION_STRING = 'omxs_large_cap_instruments'
    OMXS_MID_CAPS_COLLECTION_STRING = 'omxs_mid_cap_instruments'
    OMXS_SMALL_CAPS_COLLECTION_STRING = 'omxs_small_cap_instruments'
    FIRST_NORTH_COLLECTION_STRING = 'fn_instruments'
    FIRST_NORTH25_COLLECTION_STRING = 'fn25_instruments'
    ID_FIELD = '_id'
    SYMBOL_FIELD = 'symbol'
    INDUSTRY_FIELD = 'industry'

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]

    def get_omxs_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.OMXS_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_omxs30_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.OMXS30_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_omxspi_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.OMXSPI_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_omxs_large_cap_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.OMXS_LARGE_CAPS_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_omxs_mid_cap_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.OMXS_MID_CAPS_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_omxs_small_cap_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.OMXS_SMALL_CAPS_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_first_north_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.FIRST_NORTH_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_first_north25_instruments(self):
        return json.dumps(
            [
                x[self.SYMBOL_FIELD] for x in
                self.__client[self.FIRST_NORTH25_COLLECTION_STRING].find(
                    {}, {self.ID_FIELD: 0, self.SYMBOL_FIELD: 1}
                )
            ]
        )

    def get_omxs_industries(self):
        return json.dumps(
            self.__client[self.OMXS_COLLECTION_STRING].distinct(self.INDUSTRY_FIELD)
        )

    def get_omxs_instruments_by_industry(self, industry):
        return json.dumps(
            list(
                self.__client[self.OMXS_COLLECTION_STRING].find(
                    {self.INDUSTRY_FIELD: industry.title()}, {self.ID_FIELD: 0}
                )
            )
        ) """


if __name__ == '__main__':
    pass
