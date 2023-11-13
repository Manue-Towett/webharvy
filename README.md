# webharvy
Submits configuration files to webharvy for scraping

Requires: python 3.11+

## Setting files
- delays.txt - put the number of pages and delays in this file
- settings.ini - put the paths, output file, and schedules in this file

## Configuration
- delays:
    - put each (pages/delay in seconds) on its on row in the delays.txt in the format:

        ```<pages> pages - <delay>s```

- paths:
    - output_path: path where output files from webharvy will be saved to
    - config_files: the path where the config xml files are stored

- output_file:
    - file_write_mode: append/overwrite/update
    - file_extension: csv/tsv/xml/json

- schedules:
    - time for submission of files for each weekday in 24 hour clock system int the format:

        ```<hours>:<minutes>``` eg. 18:00

## Installing Requirements
- pip install -r requirements.txt

## Usage
- cd into the project directory
- run the script with the command ```python main.py```