import os
import dataclasses
from typing import Optional
from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    MetaData,
    String,
    Integer,
    Table,
    create_engine,
    select
)

from .logger import Logger
from .config_file import ConfigFile

class SQLHandler:
    """Saves progress to sqlite3 database"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)

        file_path = f"{os.path.abspath(os.getcwd())}/stats/progress.db"

        self.engine = create_engine(f"sqlite:///{file_path}", future=True)

        self.table = self.__create_table()

    def __create_table(self) -> Table:
        meta = MetaData()

        progress_table = Table("progress", meta,
                               Column("id", Integer, primary_key=True),
                               Column("filename", String),
                               Column("last_run", DateTime),
                               Column("weekday", String))
        
        meta.create_all(self.engine)

        return progress_table

    def __record_exists(self, file: ConfigFile) -> bool:
        with self.engine.connect() as connection:
            query = self.table.select().where(self.table.c.filename == file.filename)

            records = connection.execute(query).fetchall()

            return True if len(records) else False
    
    def __update_record(self, file: ConfigFile) -> None:
        with self.engine.connect() as connection:
            query = self.table.update().where(self.table.c.filename == file.filename)

            connection.execute(query.values(last_run = file.last_run, 
                                            weekday = file.last_run.strftime("%A")))
            
            connection.commit()
    
    def add_record(self, file: ConfigFile) -> None:
        data = dataclasses.asdict(file)

        data["weekday"] = file.last_run.strftime("%A")

        if self.__record_exists(file):
            return self.__update_record(file)

        with self.engine.connect() as connection:
            connection.execute(self.table.insert().values(**data))

            connection.commit()

    def fetch_records(self, _date: datetime) -> Optional[list[ConfigFile]]:
        with self.engine.connect() as connection:
            stmt = self.table.select().where(self.table.c.weekday==_date.strftime("%A"))
            
            records = connection.execute(stmt).fetchall()

        if len(records):
            return [ConfigFile(*record[1:-1]) for record in records]