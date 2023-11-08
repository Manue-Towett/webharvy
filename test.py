from datetime import datetime, date, timedelta

# print(datetime.now().strftime("%A"))

# import re, os

# print(os.listdir("./configs/"))

# print(re.search(r"\.[a-zA-Z]{1,6}$", 
#                 "C:\\Program.g\\icon.img\\file_10.09.23.csv").group())

today = datetime.today().strftime('%Y-%m-%d') + " 23:00"

print(datetime.strptime(today, "%Y-%m-%d %H:%M"))

date_1 = datetime.now()

import time

time.sleep(3)

date_2 = datetime.now()

y = date_1 - date_2

print(date_1, date_2)

print((date_1 - date_2).total_seconds())

# print(datetime.day)

print(datetime.now().isoweekday())

print((date_1 + timedelta(days=1)))

import os

time_modified = time.ctime(os.path.getmtime("main.py"))

print(datetime.strptime(time_modified, "%a %b %d %H:%M:%S %Y"))