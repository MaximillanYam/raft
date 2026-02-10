from pathlib import Path
import sqlite3

from .storage import BaseStorage
from .message import LogEntry

class StorageError(Exception):
    pass


class FileStorage(BaseStorage):
    def __init__(self, database_path):
        self.connection = sqlite3.connect(database_path)
        self.cursor = self.connection.cursor()
        self.id = 1  
        self._create_tables()
        self._check_metadata_table_count()

    def _create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                voted_for INTEGER
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS log (
                log_index INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                command BLOB NOT NULL
            )
        ''')
        self.connection.commit()

    def _check_metadata_table_count(self):
        self.cursor.execute("SELECT COUNT(*) FROM metadata")
        count = self.cursor.fetchone()[0]
        if count == 0:
            self.cursor.execute(
                "INSERT INTO metadata (id, term, voted_for) VALUES (?, ?, ?)",
                (self.id, 0, None)
            )
            self.connection.commit()

    def save_metadata(self, term, voted_for):
        try:
            self.cursor.execute(
                """
                UPDATE metadata
                SET term = ?, voted_for = ?
                WHERE id = ?
                """,
                (term, voted_for, self.id)
            )
            self.connection.commit()
        except Exception as e:
            raise StorageError("Failed to save metadata") from e

    def load_metadata(self):
        self.cursor.execute(
            "SELECT term, voted_for FROM metadata WHERE id = ?",
            (self.id,)
        )
        row = self.cursor.fetchone()
        if row is None:
            raise StorageError("Metadata row missing — database corrupted?")
        return row[0], row[1]

    def append_entries(self, log_entries):
        """Append new entries — does NOT allow overwriting existing indices"""
        try:
            data = [(e.index, e.term, e.command) for e in log_entries]
            self.cursor.executemany(
                "INSERT INTO log (log_index, term, command) VALUES (?, ?, ?)",
                data
            )
            self.connection.commit()
        except sqlite3.IntegrityError as e:
            raise StorageError(
                "Integrity error — tried to insert duplicate log index?"
            ) from e
        except Exception as e:
            raise StorageError("Failed to append log entries") from e

    def delete_from_index(self, index):
        """Delete all entries with log_index >= index (conflict resolution)"""
        try:
            self.cursor.execute(
                "DELETE FROM log WHERE log_index >= ?",
                (index,)
            )
            self.connection.commit()
        except Exception as e:
            raise StorageError(f"Failed to truncate log from index {index}") from e

    def get_entry(self, index):
        self.cursor.execute(
            "SELECT log_index, term, command FROM log WHERE log_index = ?",
            (index,)
        )
        row = self.cursor.fetchone()
        if row:
            return LogEntry(index=row[0], term=row[1], command=row[2])
        return None

    def get_entries_from(self, index):
        self.cursor.execute(
            "SELECT log_index, term, command FROM log WHERE log_index >= ? ORDER BY log_index ASC",
            (index,)
        )
        rows = self.cursor.fetchall()
        return [LogEntry(index=r[0], term=r[1], command=r[2]) for r in rows]

    def last_index(self):
        self.cursor.execute(
            "SELECT MAX(log_index) FROM log"
        )
        result = self.cursor.fetchone()[0]
        return result if result is not None else 0

    def last_term(self):
        self.cursor.execute(
            "SELECT term FROM log ORDER BY log_index DESC LIMIT 1"
        )
        result = self.cursor.fetchone()
        return result[0] if result else 0


def main():
    store = FileStorage("test.db")

    store.save_metadata(5, 1)
    term, voted = store.load_metadata()
    print(f"Metadata → term={term}, voted_for={voted}")
    assert (term, voted) == (5, 1)


    entries = [
        LogEntry(1, 1, b"set x 10"),
        LogEntry(2, 1, b"set y 20"),
        LogEntry(3, 2, b"append z hello"),
    ]
    store.append_entries(entries)

    print(f"Last index: {store.last_index()}")    
    print(f"Last term:  {store.last_term()}")     

    entry = store.get_entry(2)
    print(f"Entry at 2: {entry}")

    store.delete_from_index(2)
    print(f"After delete_from(2) → last index = {store.last_index()}")  # 1
    print(f"Entries from 1: {store.get_entries_from(1)}")

    store.append_entries([LogEntry(2, 3, b"new conflicting entry")])
    print(f"After re-append → last index = {store.last_index()}")  # 2


if __name__ == "__main__":
    main()