from abc import ABCMeta, abstractmethod

from doc_database_meta_classes.tet_signals_doc_db import ITetSignalsDocumentDatabase


class ITetSystemsDocumentDatabase(ITetSignalsDocumentDatabase):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_system_metrics(self):
        ...

    @abstractmethod
    def get_entry_signal_data(sefl):
        ...

    @abstractmethod
    def get_active_position_data(self):
        ...

    @abstractmethod
    def get_exit_signal_data(self):
        ...

    @abstractmethod
    def get_system_signals_for_symbol(self):
        ...

    @abstractmethod
    def insert_position_list(self):
        ...

    @abstractmethod
    def get_position_list(self):
        ...

    @abstractmethod
    def insert_single_symbol_position_list(self):
        ...

    @abstractmethod
    def get_single_symbol_position_list(self):
        ...

    @abstractmethod
    def get_historic_data(self):
        ...

    @abstractmethod
    def get_single_symbol_historic_data(self):
        ...
