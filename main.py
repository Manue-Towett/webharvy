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

DELAY_SETTINGS_PATH = "./settings/delays.txt"

DELAY_RE = re.compile(r"\b(\d+)\spage.+\b(\d+)s", re.I)

FILENAME_RE = re.compile(r"(.+?)_([a-zA-Z0-9\-\*'\"]+?)_(\d+).*.xml", re.I)

config = configparser.ConfigParser()

with open("./settings/settings.ini") as file:
    config.read_file(file)

OUTPUT_FILE_PATH = config.get("paths", "output_path")

CONFIG_FILES_PATH = config.get("paths", "config_files")

FILE_EXTENSION = config.get("output_file", "file_extension")

FILE_WRITE_MODE = config.get("output_file", "file_write_mode")

THREAD_NUM = int(config.get("concurrency", "thread_num"))

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

@dataclasses.dataclass
class Files:
    website: str
    config_path: str
    files: Optional[list[str]] = dataclasses.field(default_factory=list)

class WebHarvyScraper:
    """Submits configuration files to webharvy app"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("{:*^50}".format(f"{__class__.__name__} Started"))

        self.stats: list[FileStats] = []
        
        self.website_queue = Queue()
        self.submit_file_queue = Queue()

        self.__start_workers()
        
        self.sql = SQLHandler()
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
    def __get_files(config_path: str, unsubmitted: list[str]) -> list[Files]:
        files: list[Files] = []
        processed: list[str] = []

        for file in os.listdir(config_path):
            if not file in unsubmitted: continue

            file_re = FILENAME_RE.search(file)

            file_path = f"{config_path}{file}"

            if file_re and file_re.group(1) not in processed:
                config_file = Files(website=file_re.group(1), config_path=config_path, files=[file_path])

                processed.append(config_file.website)

                files.append(config_file)

            elif file_re:
                [f.files.append(file_path) for f in files if file_re.group(1) == f.website]

        return files
    
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
    
    def __get_config_files(self, weekday: str) -> list[Files]:
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
                    unsubmitted.append(file)
        
        if len(unsubmitted):
            config_files = self.__get_files(config_path, unsubmitted)

            random.shuffle(config_files)

            self.logger.info(f"{len(config_files)} websites ({len(unsubmitted)} files) scheduled for submission")
        
            return config_files
    
    def __check_modification_time(self, file_path: str) -> datetime:
        time_modified = time.ctime(os.path.getmtime(file_path))

        return datetime.strptime(time_modified, "%a %b %d %H:%M:%S %Y")
    
    def __submit_file(self) -> None:
        while True:
            file_args = self.submit_file_queue.get()

            os.system(f"{WEBHARVY_EXE_PATH} {file_args}")

            self.submit_file_queue.task_done()
    
    def __process_config_files(self, files: Files) -> None:
        name = threading.current_thread().name.split(" ")[0]

        for file in files.files:
            file_path, file_name = file, file.split(files.config_path)[-1]

            self.logger.info("%s: Submitting file %s" % (name, file_name))

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

            self.__wait_for_processed_file(name, output_object)

            self.sql.add_record(ConfigFile(file_name, datetime.now()))

            if file != files.files[-1]:
                self.logger.info("%s: Next file to be submitted in %ss" % (name, delay))

            time.sleep(delay)

    def __work_website(self) -> None:
        while True:
            job: Files = self.website_queue.get()

            self.__process_config_files(job)
            
            self.website_queue.task_done()

    def __wait_for_processed_file(self, name: str, output_file: OutputFile) -> None:
        while True:
            if not os.path.isfile(output_file.filepath):continue

            last_modification = self.__check_modification_time(output_file.filepath)

            if output_file.last_modified is not None:
                difference = last_modification - output_file.last_modified

                if difference.total_seconds() <= 0: continue
            
            difference = datetime.now() - last_modification

            if difference.total_seconds() < 30: continue

            self.logger.info("%s: Removing null values in %s" % (name, output_file.filename))

            self.data_handler.queue.put((name, output_file.filepath))

            return self.data_handler.queue.join()

    def __create_queue(self, unsubmitted_files: list[dict[str, str]]) -> None:
        self.last_file = unsubmitted_files[-1]

        [self.website_queue.put(file) for file in unsubmitted_files] 
        
        self.website_queue.join()

    def __start_workers(self) -> None:
        [threading.Thread(target=self.__work_website, daemon=True).start() for _ in range(THREAD_NUM)]

        [threading.Thread(target=self.__submit_file, daemon=True).start() for _ in range(THREAD_NUM)]

    def run(self) -> None:
        past_schedules = []

        while True:
            date_schedule = self.__get_next_schedule_time(dt.today() in past_schedules)

            total_seconds = date_schedule.total_seconds()

            total_minutes = total_seconds / 60

            if date_schedule.days < 0 or total_minutes < 0:
                total_minutes = abs(total_minutes)

                hours = int(total_minutes / 60)

                minutes = int(total_minutes - (hours * 60))

                seconds = int(abs(total_seconds) - ((hours * 3600) + (minutes * 60)))

                if minutes % 10 == 0:
                    time_remaining = f"{hours} hours {minutes} minutes {seconds} seconds"

                    self.logger.info(f"{time_remaining} remaining to the next available job")
                    
                    time.sleep(60)

                continue
            
            weekday = self.__get_weekday()

            past_schedules.append(dt.today())
            
            unsubmitted_files = self.__get_config_files(weekday)

            if unsubmitted_files is None: continue

            self.logger.info("Scheduled time reached for {}".format(weekday))

            self.__create_queue(unsubmitted_files)


if __name__ == "__main__":
    app = WebHarvyScraper()
    app.run()