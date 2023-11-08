import re
import threading
import dataclasses
from queue import Queue
from typing import Optional

import pandas as pd

from utils import Logger

COLUMNS = ["Title", "Url", "Image", "Price"]

NON_NULL_COLUMNS = ["Title", "Image", "Price"]

@dataclasses.dataclass
class Columns:
    """Stores columns information"""
    title: str
    url: str
    image: str
    price: str

@dataclasses.dataclass
class FileStats:
    """Store file descriptions i.e. file name, products before and products after"""
    website: str
    category: str
    output_path: str
    products_count_before: Optional[int] = None
    products_count_after: Optional[int] = None

class DataHandler:
    """Removes any rows with blank values for title, price or image"""
    def __init__(self, stats: list[FileStats]) -> None:
        self.logger = Logger(__class__.__name__)

        self.queue = Queue()

        self.stats = stats

        [threading.Thread(target=self.__work, daemon=True).start() for _ in range(3)]

    @staticmethod
    def __get_columns(columns: list[str]) -> Optional[Columns]: 
        for column in columns:
            if re.search(r"title", column, re.I):
                title = column
            elif re.search(r"url", column, re.I):
                url = column
            elif re.search(r"image", column, re.I):
                image = column
            elif re.search(r"price", column, re.I):
                price = column
        
        try:
            return Columns(title=title, url=url, image=image, price=price)
        except: pass

    @staticmethod
    def __rename_columns(df: pd.DataFrame, columns: Columns) -> pd.DataFrame:
        return df.rename(columns={columns.title: COLUMNS[0],
                                  columns.url: COLUMNS[1],
                                  columns.image: COLUMNS[2],
                                  columns.price: COLUMNS[-1]})
    
    def __read_file(self, file_path: str) -> Optional[pd.DataFrame]:
        if re.search(r".xlsx$", file_path, re.I):
            return pd.read_excel(file_path)
        elif re.search(r".csv$", file_path, re.I):
            return pd.read_csv(file_path)
        elif re.search(r".json$", file_path, re.I):
            return pd.read_json(file_path)
        elif re.search(r".xml$", file_path, re.I):
            return pd.read_xml(file_path)
        elif re.search(r".tsv$", file_path, re.I):
            return pd.read_csv(file_path, sep="\t")
        
        extension = re.search(r"\.[a-zA-Z]{1,6}$", file_path)

        self.logger.warn("Skipping removal of blanks as the script is not "
                         "configured to handle {} file type".format(extension))
    
    def __save(self, df: pd.DataFrame, file_path: str) -> None:
        if re.search(r".xlsx$", file_path, re.I):
            df.to_excel(file_path, index=False)
        elif re.search(r".csv$", file_path, re.I):
            df.to_csv(file_path, index=False)
        elif re.search(r".json$", file_path, re.I):
            df.to_json(file_path, index=False)
        elif re.search(r".xml$", file_path, re.I):
            df.to_xml(file_path, index=False)
        elif re.search(r".tsv$", file_path, re.I):
            df.to_csv(file_path, sep="\t")
        
        filename = file_path

        if re.search(r"\/", file_path):
            filename = file_path.split("/")[-1]
        elif re.search(r"\\", file_path):
            filename = file_path.split("\\")[-1]

        stats = [dataclasses.asdict(record) for record in self.stats]

        [record.pop("output_path") for record in stats]

        pd.DataFrame(stats).to_csv("./stats/stats.csv", index=False)

        self.logger.info("Non-null records saved to {}".format(filename))
    
    def __work(self) -> None:
        while True:
            output_path = self.queue.get()

            stats = self.__get_stats(output_path)

            df =  self.__read_file(output_path)

            stats.products_count_before = len(df)

            if df is None:
                self.queue.task_done()

                continue

            columns = self.__get_columns(list(df.columns.values))

            df = self.__rename_columns(df, columns)

            [df.dropna(subset=column, inplace=True) for column in NON_NULL_COLUMNS]

            stats.products_count_after = len(df)

            self.__save(df, output_path)

            self.queue.task_done()
    
    def __get_stats(self, output_path: str) -> FileStats:
        for stats in self.stats:
            if stats.output_path == output_path:
                return stats