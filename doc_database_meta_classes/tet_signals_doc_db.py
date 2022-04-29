from abc import ABCMeta, abstractmethod


class ITetSignalsDocumentDatabase(metaclass=ABCMeta):
    
    __metaclass__ = ABCMeta

    @abstractmethod
    def _insert_system(self):
        ...

    @abstractmethod
    def _get_system_id(self):
        ...

    @abstractmethod
    def get_systems(self):
        ...

    @abstractmethod
    def insert_system_metrics(self):
        ...

    @abstractmethod
    def insert_entry_signal_data(self):
        ...

    @abstractmethod
    def insert_active_position_data(self):
        ...

    @abstractmethod
    def insert_exit_signal_data(self):
        ...
