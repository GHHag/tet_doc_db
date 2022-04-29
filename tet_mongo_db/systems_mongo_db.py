import json
import pickle

import pymongo

from doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase

from TETrading.position.position_manager import PositionManager


class TetSystemsMongoDb(ITetSystemsDocumentDatabase):

    def __init__(self, client_uri, client_name):
        mongo_client = pymongo.MongoClient(client_uri)
        self.__client = mongo_client[client_name]

    def _insert_system(self, system_name):
        self.__client['systems'].insert_one({'name': system_name})

    def _get_system_id(self, system_name):
        query = self.__client['systems'].find_one(
            {'name': system_name},
            {'_id': 1}
        )
        return query['_id'] if query else None

    def get_systems(self):
        query = self.__client['systems'].find(
            {}, 
            {'_id': 0, 'name': 1}
        )
        return json.dumps(list(query))

    def insert_system_metrics(self, system_name, metrics, *args):
        system_id = self._get_system_id(system_name)
        if not system_id:
            return False
        else:
            result = self.__client['systems'].update_one(
                {'_id': system_id, 'name': system_name},
                {'$set': {'metrics': {k: v for k, v in metrics.items()}}}
            )
            for data_dict in list(args):
                if isinstance(data_dict, dict):
                    for k, v in data_dict.items():
                        self.__client['systems'].update_one(
                            {'_id': system_id, 'name': system_name},
                            {'$set': {k: v}}
                        )
            return result.modified_count > 0

    def get_system_metrics(self, system_name):
        query = self.__client['systems'].find_one(
            {'name': system_name},
            {'_id': 0, 'name': 1, 'metrics': 1, 'num_of_periods': 1}
        )
        return json.dumps(query)

    def insert_entry_signal_data(self, system_name, data):
        data = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        self.__client[f'{system_name}_entry_signals'].remove({})
        for data_p in data['data']:
            data_p['system_id'] = system_id
            self.__client[f'{system_name}_entry_signals'].insert_one(data_p)
        return True

    def get_entry_signal_data(self, system_name):
        query = self.__client[f'{system_name}_entry_signals'].find(
            {}, 
            {'_id': 0, 'index': 0, 'signal_index': 0, 'system_id': 0}
        )
        return json.dumps(list(query))

    def insert_active_position_data(self, system_name, data):
        data = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        self.__client[f'{system_name}_active_pos'].remove({})
        for data_p in data['data']:
            data_p['system_id'] = system_id
            self.__client[f'{system_name}_active_pos'].insert_one(data_p)
        return True

    def get_active_position_data(self, system_name):
        query = self.__client[f'{system_name}_active_pos'].find(
            {}, 
            {'_id': 0, 'index': 0, 'signal_index': 0, 'system_id': 0}
        )
        return json.dumps(list(query))

    def insert_exit_signal_data(self, system_name, data):
        data = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        self.__client[f'{system_name}_exit_signals'].remove({})
        for data_p in data['data']:
            data_p['system_id'] = system_id
            self.__client[f'{system_name}_exit_signals'].insert_one(data_p)
        return True

    def get_exit_signal_data(self, system_name):
        query = self.__client[f'{system_name}_exit_signals'].find(
            {}, 
            {'_id': 0, 'index': 0, 'signal_index': 0, 'system_id': 0}
        )
        return json.dumps(list(query))

    def get_system_signals_for_symbol(self, system_name, symbol):
        system_id = self._get_system_id(system_name)
        query = self.__client[f'{system_name}_entry_signals'].find(
            {'system_id': system_id, 'symbol': symbol}, {'_id': 0, 'system_id': 0}
        ), \
        self.__client[f'{system_name}_active_pos'].find(
            {'system_id': system_id, 'symbol': symbol}, {'_id': 0, 'system_id': 0}
        ), \
        self.__client[f'{system_name}_exit_signals'].find(
            {'system_id': system_id, 'symbol': symbol}, {'_id': 0, 'system_id': 0}
        )
        return json.dumps(list(map(list, query)))

    def insert_position_list(self, system_name, position_list):
        system_id = self._get_system_id(system_name)
        result = self.__client['systems'].update_one(
            {'_id': system_id, 'name': system_name},
            {'$set': {'positions': [pickle.dumps(pos) for pos in position_list]}}
        )
        return result.modified_count > 0

    def get_position_list(self, system_name):
        system_id = self._get_system_id(system_name)
        query = self.__client['systems'].find_one(
            {'_id': system_id, 'name': system_name},
            {'_id': 0, 'positions': 1}
        )
        return list(map(pickle.loads, query['positions']))

    def insert_single_symbol_position_list(
        self, system_name, symbol, position_list, num_of_periods
    ):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        result = self.__client['positions'].update_one(
            {'system_id': system_id, 'system_name': system_name, 'symbol': symbol},
            {'$set': {
                'positions': [pickle.dumps(pos) for pos in position_list],
                'num_of_periods': num_of_periods
                }
            }, upsert=True
        )
        return result.modified_count > 0

    def get_single_symbol_position_list(self, system_name, symbol):
        system_id = self._get_system_id(system_name)
        query = self.__client['positions'].find_one(
            {'system_id': system_id, 'system_name': system_name, 'symbol': symbol},
            {'_id': 0, 'positions': 1, 'num_of_periods': 1}
        )
        query['positions'] = list(map(pickle.loads, query['positions']))
        return query['positions'], query['num_of_periods']

    def get_historic_data(self, system_name):
        position_list = self.get_position_list(system_name)
        system_metrics = json.loads(self.get_system_metrics(system_name))

        start_dt = position_list[0].entry_dt
        end_dt = position_list[-1].exit_signal_dt if position_list[-1].exit_signal_dt is not None \
            else position_list[-2].exit_signal_dt

        position_manager = PositionManager(
            system_name, system_metrics['num_of_periods'], 10000, 1.0
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
        position_list, num_of_periods = self.get_single_symbol_position_list(system_name, symbol)

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
        self.__client['ml_models'].update_one(
            {'system_id': system_id, 'name': system_name, 'instrument': instrument},
            {'$set': {'model': model}}, upsert=True
        )
        return True

    def get_ml_model(self, system_name, instrument):
        query = self.__client['ml_models'].find_one(
            {'name': system_name, 'instrument': instrument}, 
            {'_id': 0, 'model': 1}
        )
        return pickle.loads(query['model'])


if __name__ == '__main__':
    pass
