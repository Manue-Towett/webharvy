import os
import re
import time
import random
import threading
import dataclasses
import configparser
from queue import Queue
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from datetime import datetime, timedelta, date as dt

from apps import DataHandler, FileStats
from utils import Logger, SQLHandler, ConfigFile

BASE_DIR = Path(__file__).resolve().parent

DELAY_RE = re.compile(r"\b(\d+)\spage.+\b(\d+)s", re.I)

FILENAME_RE = re.compile(r"(.+?)_([a-zA-Z0-9\-]+?)_(\d+).*.xml", re.I)

config = configparser.ConfigParser()

with open("./settings/settings.ini") as file:
    config.read_file(file)

OUTPUT_FILE_PATH = config.get("paths", "output_path")

CONFIG_FILES_PATH = config.get("paths", "config_files")

DELAY_SETTINGS_PATH = config.get("paths", "pages_delays")

FILE_EXTENSION = config.get("output_file", "file_extension")

FILE_WRITE_MODE = config.get("output_file", "file_write_mode")

WEBHARVY_EXE_PATH = os.path.join(os.getenv("APPDATA"), "SysNucleus\\WebHarvy\\WebHarvy.exe")

@dataclasses.dataclass
class Schedule:
    Monday: str = config.get("schedules", "monday")
    Tuesday: str = config.get("schedules", "tuesday")
    Wednesday: str = config.get("schedules", "wednesday")
    Thursday: str = config.get("schedules", "thursday")
    Friday: str = config.get("schedules", "friday")
    Saturday: str = config.get("schedules", "saturday")
    Sunday: str = config.get("schedules", "sunday")

@dataclasses.dataclass
class OutputFile:
    filename: str
    filepath: str
    last_modified: Optional[datetime] = None

class WebHarvyScraper:
    """Submits configuration files to webharvy app"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("{:*^50}".format(f"{__class__.__name__} Started"))

        self.stats: list[FileStats] = []
        self.processed_files: list[str] = []
        self.unprocessed_files: list[OutputFile] = []
        
        self.sql = SQLHandler()
        self.submit_queue = Queue()
        self.submit_file_queue = Queue()
        self.data_handler = DataHandler(self.stats)

        self.delays = self.__read_delay_settings()
    
    def __read_delay_settings(self) -> dict[int, int]:
        delays = {}

        with open(DELAY_SETTINGS_PATH) as file:
            for line in file.readlines():
                page_delay = DELAY_RE.search(line)

                if page_delay is None: continue

                delays[int(page_delay.group(1))] = int(page_delay.group(2))
        
        return delays
    
    @staticmethod
    def __get_weekday(date_: Optional[datetime]=None) -> str:
        if date_ is None:
            date_ = datetime.now()

        return date_.strftime("%A")
    
    def __get_config_path(self) -> Optional[str]:
        today = self.__get_weekday()

        for day in os.listdir(CONFIG_FILES_PATH):
            if re.search(today, day, re.I):
                return f"{CONFIG_FILES_PATH}{day}/"
    
    def __get_status(self, file: str, submitted: list[ConfigFile], weekday: str) -> bool:
        date_schedule = self.__get_date_difference(weekday)

        for config_file in submitted:
            if file == config_file.filename:
                return date_schedule <= config_file.last_run
        
        return False
    
    def __get_delay(self, pages: int) -> int:
        page_nums = list(self.delays.keys())

        page_nums.sort()

        for page_num in page_nums:
            if page_num > pages:
                return self.delays[page_num]
        
        return self.delays[page_num]
    
    @staticmethod
    def __get_date() -> str:
        today = datetime.today()

        return f"{today.month}.{today.day}.{str(today.year)[2:]}"
    
    @staticmethod
    def __get_name(domain_slugs: list[str]) -> str:
        if len(domain_slugs) == 2:
            domain = domain_slugs[0] 
        else:
            domain = domain_slugs[1] 
        
        return domain
    
    @staticmethod
    def __get_date_difference(weekday: str) -> datetime:
        time_schedule = dataclasses.asdict(Schedule()).get(weekday)

        today_schedule = f"{datetime.today().strftime('%Y-%m-%d')} {time_schedule}"

        date_schedule = datetime.strptime(today_schedule, "%Y-%m-%d %H:%M")

        return date_schedule
    
    def __get_next_schedule_time(self, today_done: Optional[bool]=False) -> timedelta:
        weekday = self.__get_weekday()

        date_schedule = self.__get_date_difference(weekday)

        if datetime.now() > date_schedule and today_done:
            weekday = self.__get_weekday(datetime.now() + timedelta(days=1))

            date_schedule = self.__get_date_difference(weekday) + timedelta(days=1)

        return datetime.now() - date_schedule
    
    def __get_config_files(self, weekday: str) -> Optional[list[dict[str, str]]]:
        unsubmitted = []

        submitted = self.sql.fetch_records(datetime.now())

        config_path = self.__get_config_path()

        if config_path is None: return

        for file in os.listdir(config_path):
            has_been_submitted = False

            if file.endswith(".xml"):
                if submitted is not None:
                    has_been_submitted = self.__get_status(file, submitted, weekday)
                
                if not has_been_submitted:
                    unsubmitted.append({file: f"{config_path}{file}"})
        
        if len(unsubmitted):
            random.shuffle(unsubmitted)

            self.logger.info("{} files scheduled for submission".format(len(unsubmitted)))
        
            return unsubmitted
    
    def __check_modification_time(self, file_path: str) -> datetime:
        time_modified = time.ctime(os.path.getmtime(file_path))

        return datetime.strptime(time_modified, "%a %b %d %H:%M:%S %Y")
    
    def __submit_file(self) -> None:
        while True:
            file_args = self.submit_file_queue.get()

            os.system(f"{WEBHARVY_EXE_PATH} {file_args}")

            self.submit_file_queue.task_done()

    def __submit(self) -> None:
        while True:
            job: dict[str, str] = self.submit_queue.get()

            file_name, file_path = list(job.items())[0]

            self.logger.info("Submitting file %s" % file_name)

            output_file_re = FILENAME_RE.search(file_name)

            website, category, page_count = output_file_re.groups()

            try:
                domain = self.__get_name(urlparse(website).netloc.split("."))

            except:
                domain = website

                if "." in website:
                    domain = self.__get_name(website.split("."))

            delay = self.__get_delay(int(page_count))   

            date = self.__get_date() 

            output_file = f"{domain}_{category}_{date}.{FILE_EXTENSION}"  

            output_path = OUTPUT_FILE_PATH + output_file

            output_object = OutputFile(output_file, output_path)

            self.stats.append(FileStats(website, category, output_path=output_path))

            if os.path.isfile(output_path):
                output_object.last_modified = self.__check_modification_time(output_path)
            
            output_dir = os.path.join(BASE_DIR, output_path.lstrip(".").lstrip("/").lstrip("\\"))

            file_dir = os.path.join(BASE_DIR, file_path.lstrip(".").lstrip("/").lstrip("\\"))

            self.submit_file_queue.put(f"{file_dir} {int(page_count)} {output_dir} {FILE_WRITE_MODE}")

            self.unprocessed_files.append(output_object)

            self.sql.add_record(ConfigFile(file_name, datetime.now()))

            if job != self.last_file:
                self.logger.info("Next file to be submitted in %ss" % delay)

            time.sleep(delay)

            self.submit_queue.task_done()

    def __get_processed_files(self) -> Optional[list[str]]:
        while True:
            files_to_remove = []

            for file in self.unprocessed_files:
                if not os.path.isfile(file.filepath):continue

                last_modification = self.__check_modification_time(file.filepath)

                if file.last_modified is not None:
                    difference = last_modification - file.last_modified

                    if difference.total_seconds() <= 0: continue
                
                difference = datetime.now() - last_modification

                if difference.total_seconds() < 120: continue

                self.logger.info("Removing null values in %s" % file.filename)

                self.data_handler.queue.put(file.filepath)

                self.data_handler.queue.join()

                files_to_remove.append(file)

                self.processed_files.append(file.filename)
            
            [self.unprocessed_files.remove(file) for file in files_to_remove]

    def __process_files(self, unsubmitted_files: list[dict[str, str]]) -> None:
        self.last_file = unsubmitted_files[-1]

        [self.submit_queue.put(file) for file in unsubmitted_files] 
        
        self.submit_queue.join()

    def __start_workers(self) -> None:
        threading.Thread(target=self.__submit, daemon=True).start()

        threading.Thread(target=self.__get_processed_files, daemon=True).start()

        [threading.Thread(target=self.__submit_file, daemon=True).start() for _ in range(10)]

    def run(self) -> None:
        self.__start_workers()

        past_schedules = []

        while True:
            date_schedule = self.__get_next_schedule_time(dt.today() in past_schedules)

            total_seconds = date_schedule.total_seconds()

            total_minutes = total_seconds / 60

            if date_schedule.days < 0 or total_minutes < 0:
                total_minutes = abs(total_minutes)

                hours = int(total_minutes / 60)

                minutes = int(total_minutes - hours * 60)

                if minutes % 10 == 0:
                    self.logger.info(f"{hours} hours {minutes} minutes remaining to the next available job")
                    
                    time.sleep(60)

                    continue
            
            weekday = self.__get_weekday()

            past_schedules.append(dt.today())
            
            unsubmitted_files = self.__get_config_files(weekday)

            if unsubmitted_files is None: continue

            self.logger.info("Scheduled time reached for {}".format(weekday))

            self.__process_files(unsubmitted_files)


if __name__ == "__main__":
    app = WebHarvyScraper()
    app.run()