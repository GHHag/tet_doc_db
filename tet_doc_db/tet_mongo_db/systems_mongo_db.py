import json
import pickle
from typing import Dict, List

from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from bson import json_util

from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase

from TETrading.position.position import Position
from TETrading.position.position_manager import PositionManager


class TetSystemsMongoDb(ITetSystemsDocumentDatabase):

    ID_FIELD = '_id'
    SYSTEM_ID_FIELD = 'system_id'
    NAME_FIELD = 'name'
    SYSTEM_NAME_FIELD = 'system_name'
    SYMBOL_FIELD = 'symbol'
    METRICS_FIELD = 'metrics'
    MARKET_STATE_FIELD = 'market_state'
    PORTFOLIO_CREATION_DATA_FIELD = 'portfolio_creation_data'
    NUMBER_OF_PERIODS_FIELD = 'num_of_periods'
    POSITION_LIST_FIELD = 'position_list'
    ML_MODEL_FIELD = 'model'
    INSTRUMENT_FIELD = 'instrument'
    SIGNAL_DT_FIELD = 'signal_dt'

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__systems: Collection = self.__client.systems
        self.__market_states: Collection = self.__client.market_states
        self.__positions: Collection = self.__client.positions
        self.__single_symbol_positions: Collection = self.__client.single_symbol_positions
        self.__ml_models: Collection = self.__client.ml_models

    @property
    def client(self):
        return self.__client

    def _insert_system(self, system_name):
        self.__systems.insert_one({self.NAME_FIELD: system_name})

    def _get_system_id(self, system_name):
        query = self.__systems.find_one(
            {self.NAME_FIELD: system_name},
            {self.ID_FIELD: 1}
        )
        return query[self.ID_FIELD] if query else None

    def get_systems(self):
        query = self.__systems.find(
            {}, 
            {self.ID_FIELD: 1, self.NAME_FIELD: 1}
        )
        return json.dumps(list(query), default=json_util.default)

    def insert_system_metrics(self, system_name, metrics: Dict, *args):
        system_id = self._get_system_id(system_name)
        if not system_id:
            return False
        else:
            result = self.__systems.update_one(
                {self.ID_FIELD: system_id, self.NAME_FIELD: system_name},
                {'$set': {self.METRICS_FIELD: {k: v for k, v in metrics.items()}}}
            )
            for data_dict in list(args):
                if isinstance(data_dict, dict):
                    for k, v in data_dict.items():
                        self.__systems.update_one(
                            {self.ID_FIELD: system_id, self.NAME_FIELD: system_name},
                            {'$set': {k: v}}
                        )
            return result.modified_count > 0

    def get_system_metrics(self, system_name):
        system_id = self._get_system_id(system_name)
        query = self.__systems.find_one(
            {self.ID_FIELD: system_id, self.NAME_FIELD: system_name},
            {
                self.ID_FIELD: 1, self.NAME_FIELD: 1, self.METRICS_FIELD: 1, 
                self.NUMBER_OF_PERIODS_FIELD: 1
            }
        )
        return json.dumps(query, default=json_util.default)

    def get_system_portfolio_creation_data(self, system_name):
        system_id = self._get_system_id(system_name)
        query = self.__systems.find_one(
            {self.ID_FIELD: system_id, self.NAME_FIELD: system_name},
            {self.ID_FIELD: 0, self.PORTFOLIO_CREATION_DATA_FIELD: 1}
        )
        return json.dumps(query[self.PORTFOLIO_CREATION_DATA_FIELD])

    def insert_market_state_data(self, system_name, data):
        data = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        for data_p in data['data']:
            assert isinstance(data_p, dict)
            data_p.update({self.SYSTEM_ID_FIELD: system_id})
            if data_p[self.MARKET_STATE_FIELD] == 'entry':
                self.__market_states.remove(
                    {self.SYSTEM_ID_FIELD: system_id, self.SYMBOL_FIELD: data_p['symbol']}
                )
                self.__market_states.insert_one(data_p)
            else:
                self.__market_states.update_one(
                    {self.SYSTEM_ID_FIELD: system_id, self.SYMBOL_FIELD: data_p['symbol']},
                    {'$set': data_p}, upsert=True
                )
        return True

    def get_market_state_data(self, system_name, market_state):
        system_id = self._get_system_id(system_name)
        query = self.__market_states.find(
            {self.SYSTEM_ID_FIELD: system_id, self.MARKET_STATE_FIELD: market_state}
        )
        return json.dumps(list(query), default=json_util.default)

    def get_market_state_data_for_symbol(self, system_name, symbol):
        system_id = self._get_system_id(system_name)
        query = self.__market_states.find_one(
            {self.SYSTEM_ID_FIELD: system_id, self.SYMBOL_FIELD: symbol},
            {self.ID_FIELD: 0, self.MARKET_STATE_FIELD: 1, self.SIGNAL_DT_FIELD: 1}
        )
        if not query:
            return json.dumps({self.MARKET_STATE_FIELD: None, self.SIGNAL_DT_FIELD: None})
        else:
            return json.dumps(query, default=json_util.default)

    def insert_position_list(
        self, system_name, position_list: List[Position], 
        format='serialized'
    ):
        system_id = self._get_system_id(system_name)
        if format == 'serialized':
            result = self.__positions.update_one(
                {self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name},
                {
                    '$set': {
                        self.POSITION_LIST_FIELD: [pickle.dumps(pos) for pos in position_list]
                    }
                }, upsert=True
            )
            return result.modified_count > 0
        elif format == 'json':
            result = self.__positions.update_one(
                {self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name},
                {
                    '$set': {
                        f'{self.POSITION_LIST_FIELD}_json': [pos.to_dict for pos in position_list]
                    }
                }, upsert=True
            )
            return result.modified_count > 0

    def get_position_list(self, system_name, format='serialized'):
        system_id = self._get_system_id(system_name)
        if format == 'serialized':
            query = self.__positions.find_one(
                {self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name},
                {self.ID_FIELD: 0, self.SYSTEM_ID_FIELD: 1, self.POSITION_LIST_FIELD: 1}
            )
            return list(map(pickle.loads, query[self.POSITION_LIST_FIELD]))
        elif format == 'json':
            query = self.__positions.find_one(
                {self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name},
                {self.ID_FIELD: 0, self.SYSTEM_ID_FIELD: 1, f'{self.POSITION_LIST_FIELD}_json': 1}
            )
            return json.dumps(query, default=json_util.default)

    def insert_single_symbol_position_list(
        self, system_name, symbol, position_list: List[Position], num_of_periods,
        format='serialized'
    ):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        if format == 'serialized':
            result = self.__single_symbol_positions.update_one(
                {
                    self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name, 
                    self.SYMBOL_FIELD: symbol
                },
                {
                    '$set': {
                        self.POSITION_LIST_FIELD: [pickle.dumps(pos) for pos in position_list],
                        self.NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0
        elif format == 'json':
            result = self.__single_symbol_positions.update_one(
                {
                    self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name, 
                    self.SYMBOL_FIELD: symbol
                },
                {
                    '$set': {
                        f'{self.POSITION_LIST_FIELD}_json': [pos.to_dict for pos in position_list],
                        self.NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0

    def get_single_symbol_position_list(
        self, system_name, symbol, 
        format='serialized', return_num_of_periods=False
    ):
        system_id = self._get_system_id(system_name)
        if format == 'serialized':
            query = self.__single_symbol_positions.find_one(
                {
                    self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name, 
                    self.SYMBOL_FIELD: symbol
                },
                {self.ID_FIELD: 0, self.POSITION_LIST_FIELD: 1, self.NUMBER_OF_PERIODS_FIELD: 1}
            )
            if return_num_of_periods:
                return list(map(pickle.loads, query[self.POSITION_LIST_FIELD])), \
                    query[self.NUMBER_OF_PERIODS_FIELD]
            else:
                return list(map(pickle.loads, query[self.POSITION_LIST_FIELD]))
        elif format == 'json':
            query = self.__single_symbol_positions.find_one(
                {
                    self.SYSTEM_ID_FIELD: system_id, self.SYSTEM_NAME_FIELD: system_name, 
                    self.SYMBOL_FIELD: symbol
                },
                {f'{self.POSITION_LIST_FIELD}_json': 1, self.NUMBER_OF_PERIODS_FIELD: 1}
            )
            return json.dumps(query, default=json_util.default)

    def get_historic_data(self, system_name):
        position_list = self.get_position_list(system_name)
        system_metrics = json.loads(self.get_system_metrics(system_name))

        start_dt = position_list[0].entry_dt
        end_dt = position_list[-1].exit_signal_dt if position_list[-1].exit_signal_dt is not None \
            else position_list[-2].exit_signal_dt

        position_manager = PositionManager(
            system_name, system_metrics[self.NUMBER_OF_PERIODS_FIELD], 10000, 1.0
        )

        def generate_pos_sequence(position_list, **kwargs):
            for pos in position_list:
                yield pos

        if position_list[-1].active_position or position_list[-1].entry_dt is None:
            position_manager.generate_positions(generate_pos_sequence, position_list[:-1])
        else:
            position_manager.generate_positions(generate_pos_sequence, position_list[:])

        return json.dumps(
            {
                'start_dt': str(start_dt),
                'end_dt': str(end_dt),
                'market_to_market_returns': list(
                        map(float, position_manager.metrics.market_to_market_returns_list)
                    ),
                'equity_list': list(map(float, position_manager.metrics.equity_list)),
                'returns_list': list(position_manager.metrics.returns_list),
                'mae_list': list(position_manager.metrics.mae_list),
                'mfe_list': list(position_manager.metrics.mfe_list),
                'pos_period_lengths_list': list(position_manager.metrics.pos_period_lengths_list)
            }
        )

    def get_single_symbol_historic_data(self, system_name, symbol):
        position_list, num_of_periods = self.get_single_symbol_position_list(
            system_name, symbol, return_num_of_periods=True
        )

        start_dt = position_list[0].entry_dt
        end_dt = position_list[-1].exit_signal_dt if position_list[-1].exit_signal_dt is not None \
            else position_list[-2].exit_signal_dt

        position_manager = PositionManager(symbol, num_of_periods, 10000, 1.0)

        def generate_pos_sequence(position_list, **kwargs):
            for pos in position_list:
                yield pos

        if position_list[-1].active_position or position_list[-1].entry_dt is None:
            position_manager.generate_positions(generate_pos_sequence, position_list[:-1])
        else:
            position_manager.generate_positions(generate_pos_sequence, position_list[:])

        return json.dumps(
            {
                'start_dt': str(start_dt),
                'end_dt': str(end_dt),
                'market_to_market_returns': list(
                        map(float, position_manager.metrics.market_to_market_returns_list)
                    ),
                'equity_list': list(map(float, position_manager.metrics.equity_list)),
                'returns_list': list(position_manager.metrics.returns_list),
                'mae_list': list(position_manager.metrics.mae_list),
                'mfe_list': list(position_manager.metrics.mfe_list),
                'pos_period_lengths_list': list(position_manager.metrics.pos_period_lengths_list)
            }
        )

    def insert_ml_model(self, system_name, instrument, model):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        self.__ml_models.update_one(
            {
                self.SYSTEM_ID_FIELD: system_id, 
                self.SYSTEM_NAME_FIELD: system_name, 
                self.INSTRUMENT_FIELD: instrument
            },
            {'$set': {self.ML_MODEL_FIELD: model}}, upsert=True
        )
        return True

    def get_ml_model(self, system_name, instrument):
        system_id = self._get_system_id(system_name)
        query = self.__ml_models.find_one(
            {
                self.SYSTEM_ID_FIELD: system_id, 
                self.SYSTEM_NAME_FIELD: system_name, 
                self.INSTRUMENT_FIELD: instrument
            }, 
            {self.ID_FIELD: 0, self.ML_MODEL_FIELD: 1}
        )
        return pickle.loads(query[self.ML_MODEL_FIELD])


if __name__ == '__main__':
    pass
