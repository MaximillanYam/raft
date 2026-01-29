from pathlib import Path
import sqlite3
from .storage import BaseStorage
from .message import LogEntry

class StorageError(Exception): 
    pass

class FileStorage(BaseStorage): 
    def __init__(self, database_path): 
        self.connection = sqlite3.connect(database_path)
        self.id = 1 
        self.cursor = self.connection.cursor()
        self._create_tables()
        self._check_metadata_table_count()

    def _create_tables(self):
        create_metadata_table = '''
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY,
            term INTEGER,
            voted_for INTEGER
        )
        '''
        create_log_table = '''
        CREATE TABLE IF NOT EXISTS log (
            log_index INTEGER PRIMARY KEY,
            term INTEGER, 
            command BLOB
        )
        '''    

        self.cursor.execute(create_metadata_table)
        self.cursor.execute(create_log_table)
        self.connection.commit()

    def _check_metadata_table_count(self): 
        metadata_count_sql = '''
        SELECT count(*) FROM metadata;
        '''

        self.cursor.execute(metadata_count_sql) 
        metadata_count = self.cursor.fetchone()[0]

        if metadata_count == 0: 
            insert_default_metadata_sql = f'''
            INSERT INTO metadata (id, term, voted_for) 
            VALUES (?, ?, ?)
            '''
            self.cursor.execute(insert_default_metadata_sql, (1, 0, None))
            self.connection.commit()     

    def save_metadata(self, term, voted_for):
        try:
            save_metadata_sql = '''
            UPDATE metadata 
            SET term = ?,
            voted_for = ?
            WHERE id = ? 
            '''

            self.cursor.execute(save_metadata_sql, (term, voted_for, self.id))
            self.connection.commit()
        except Exception as e:
            raise StorageError("Failed to save metadata.") from e
        
    def load_metadata(self):
        load_metadata_sql = '''
        SELECT term, voted_for FROM metadata where id = ? 
        '''

        self.cursor.execute(load_metadata_sql, (self.id,))
        result  = self.cursor.fetchone()
        term, voted_for = result[0], result[1] 
    
        return term, voted_for

    def append_entries(self, log_entries): 
        try:
            log_entry_list = [] 

            for log_entry in log_entries: 
                log_entry_list.append((log_entry.index, log_entry.term, log_entry.command))

            append_entries_sql = '''
            INSERT OR REPLACE INTO log (log_index, term, command) VALUES (?, ?, ?) 
            '''

            self.cursor.executemany(append_entries_sql, log_entry_list) 
            self.connection.commit()
        except Exception as e: 
            raise StorageError('Failed to append entries to log') from e

    def delete_from_index(self, index):
        try:
            delete_from_index_sql = '''
            DELETE FROM log WHERE log_index >= ? 
            '''

            self.cursor.execute(delete_from_index_sql, (index,))
            self.connection.commit()
        except Exception as e: 
            raise StorageError(f'Failed to delete from index {index}') from e

    def get_entry(self, index): 
        get_entry_sql = '''
        SELECT log_index, term, command FROM log where log_index = ?
        '''

        self.cursor.execute(get_entry_sql, (index,))
        result = self.cursor.fetchone() 
        
        if result: 
            log_index = 0 
            term = 1
            command = 2 
            return LogEntry(result[log_index], result[term], result[command]) 
        
        return None
    
    def get_entries_from(self, index): 
        get_entries_from_sql = '''
        SELECT log_index, term, command FROM log where log_index >= ? 
        '''

        self.cursor.execute(get_entries_from_sql, (index,))
        result = self.cursor.fetchall()

        if result: 
            log_entries = []

            for row in result: 
                log_index = 0 
                term = 1 
                command = 2 
                log_entries.append(LogEntry(log_index, term, command)) 
            
            return log_entries 

        return []

    def last_index(self):
        last_index_sql = '''
        SELECT log_index FROM log ORDER BY log_index DESC limit 1 
        '''

        self.cursor.execute(last_index_sql) 
        result = self.cursor.fetchone()

        if result: 
            log_index = 0 
            return result[log_index] 
        
        return 0 
    
    def last_term(self): 
        last_term_sql = '''
        SELECT log_index, term FROM log ORDER BY log_index DESC limit 1
        '''

        self.cursor.execute(last_term_sql) 
        result = self.cursor.fetchone() 

        if result: 
            term = 1 
            return result[term] 
    
        return 0 



def main():
    # 1. Setup
    store = FileStorage('test.db')
    
    # 2. Test Metadata
    store.save_metadata(5, 1)
    assert store.load_metadata() == (5, 1)
    
    # 3. Test Log
    entries = [
        LogEntry(1, 1, b"cmd1"),
        LogEntry(2, 1, b"cmd2"),
        LogEntry(3, 2, b"cmd3")
    ]
    store.append_entries(entries)
    
    # 4. Check 'Last' logic
    print(f"Last Index: {store.last_index()}") # Should be 3
    print(f"Last Term: {store.last_term()}")   # Should be 2
    
    # 5. Test Conflict Deletion
    store.delete_from_index(2) 
    print(f"New Last Index: {store.last_index()}") # Should be 1

if __name__ == '__main__': 
    main()