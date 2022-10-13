import json

from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from bson import json_util, objectid

from TETrading.utils.metadata.trading_system_attributes import TradingSystemAttributes

class InstrumentsMongoDbTest:

    MARKET_LISTS_COLLECTION = 'market_lists'
    INSTRUMENTS_COLLECTION = 'instruments'

    ID_FIELD = '_id'
    MARKET_LIST_FIELD = 'market_list'
    MARKET_LIST_IDS_FIELD = 'market_list_ids'
    SYMBOL_FIELD = TradingSystemAttributes.SYMBOL 

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__market_lists: Collection = self.__client[self.MARKET_LISTS_COLLECTION]
        self.__instruments: Collection = self.__client[self.INSTRUMENTS_COLLECTION]

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

    def get_market_list_instrument_symbols(self, market_list_id):
        query = self.__market_lists.aggregate(
            [
                {
                    '$match': { self.ID_FIELD: objectid.ObjectId(market_list_id)
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

if __name__ == '__main__':
    x = InstrumentsMongoDbTest('mongodb://localhost:27017/', 'instruments_db_test')
    market_lists = []
    market_lists.append(json.loads(x.get_market_list_by_name('omxs30')))
    market_lists.append(json.loads(x.get_market_list_by_name('first_north25')))
    symbols_list = []
    for market_list in market_lists:
        market_list_instruments = json.loads(x.get_market_list_instrument_symbols(market_list['_id']['$oid']))
        symbols_list += [i['symbol'] for i in market_list_instruments['market_list_instruments']]
    print(symbols_list)