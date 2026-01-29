from abc import ABC, abstractmethod
import sqlite3

class BaseStorage(ABC): 
    @abstractmethod 
    def save_metadata(self, term, voted_for):
        pass

    @abstractmethod
    def load_metadata(self): 
        pass 

    @abstractmethod
    def append_entries(self, log_entries): 
        pass 

    @abstractmethod 
    def delete_from_index(self, index): 
        pass

    @abstractmethod 
    def get_entry(self, index):
        pass 

    @abstractmethod 
    def get_entries_from(self, index):
        pass 

    @abstractmethod
    def last_index(self): 
        pass 

    @abstractmethod
    def last_term(self): 
        pass
    
