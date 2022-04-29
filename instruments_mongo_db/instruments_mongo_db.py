import json

import pymongo


class InstrumentsMongoDb():

    def __init__(self, client_uri, client_name):
        mongo_client = pymongo.MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__OMXS_COLLECTION_STRING = 'omxs_instruments'
        self.__OMXS30_COLLECTION_STRING = 'omxs30_instruments'
        self.__OMXSPI_COLLECTION_STRING = 'omxspi_instruments'
        self.__OMXS_LARGE_CAPS_COLLECTION_STRING = 'omxs_large_cap_instruments'
        self.__OMXS_MID_CAPS_COLLECTION_STRING = 'omxs_mid_cap_instruments'
        self.__OMXS_SMALL_CAPS_COLLECTION_STRING = 'omxs_small_cap_instruments'
        self.__FIRST_NORTH_COLLECTION_STRING = 'fn_instruments'
        self.__FIRST_NORTH25_COLLECTION_STRING = 'fn25_instruments'

    def get_omxs_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__OMXS_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_omxs30_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__OMXS30_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_omxspi_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__OMXSPI_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_omxs_large_cap_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__OMXS_LARGE_CAPS_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_omxs_mid_cap_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__OMXS_MID_CAPS_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_omxs_small_cap_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__OMXS_SMALL_CAPS_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_first_north_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__FIRST_NORTH_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_first_north25_instruments(self):
        return json.dumps(
            [
                x['symbol'] for x in
                self.__client[self.__FIRST_NORTH25_COLLECTION_STRING].find(
                    {}, {'_id': 0, 'symbol': 1}
                )
            ]
        )

    def get_omxs_industries(self):
        return json.dumps(
            self.__client[self.__OMXS_COLLECTION_STRING].distinct('industry')
        )

    def get_omxs_instruments_by_industry(self, industry):
        return json.dumps(
            list(
                self.__client[self.__OMXS_COLLECTION_STRING].find(
                    {'industry': industry.title()}, {'_id': 0}
                )
            )
        )


if __name__ == '__main__':
    pass
