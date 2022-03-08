#!/usr/bin/env python3

import os
import requests
import datetime
import pytz
import csv

import json
import pathlib

# Inputs
USERNAME            = os.getenv('USER')
PASSWORD            = os.getenv('PASSWORD')
ACCOUNT_NUMBER      = os.getenv('ACCOUNT_NUMBER')
DISTRICT            = os.getenv('DISTRICT')

# Outputs
CACHE_FILE          = 'cache.json'
CACHE_EXPIRY_HOURS  = 6
MISSING_READINGS    = 6

def get_hourly_water_usage_report(username, password, account_number, district):

    # API Constants
    API_HOST            = 'https://' + DISTRICT + '.aquahawk.us'
    LOGIN_ENDPOINT      = '/login'
    EXPORT_ENDPOINT     = '/timeseries/export'
    DOWNLOAD_ENDPOINT   = '/download'

    # Report Constants
    EXPORT_OFFSET_DAYS      = 1
    EXPORT_RANGE_HOURS      = 12
    CSV_COLUMN_LAST_DAY     = -5
    CSV_COLUMN_TIMESTAMP    = -4
    CSV_COLUMN_CF           = -3
    CSV_COLUMN_GALLONS      = -2
    CSV_COLUMN_READING      = -1
    CSV_ROW_OFFSET          = 1

    login_payload = {
        'username': USERNAME, 
        'password': PASSWORD
    }
    login_request = requests.post(API_HOST + LOGIN_ENDPOINT, data=login_payload, headers={'accept': 'application/json'})

    now             = datetime.datetime.now(tz=pytz.timezone('UTC'))
    lag_from        = now - datetime.timedelta(days=EXPORT_OFFSET_DAYS)
    lag_to          = lag_from + datetime.timedelta(hours=EXPORT_RANGE_HOURS)
    lag_from_str    = datetime.datetime(lag_from.year, lag_from.month, lag_from.day, lag_from.hour).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    lag_to_str      = datetime.datetime(lag_to.year, lag_to.month, lag_to.day, lag_to.hour, 59).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    export_payload  = {
        'firstTime':        lag_from_str,
        'lastTime':         lag_to_str,
        'interval':         '1 hour',
        'districtName':     DISTRICT,
        'accountNumber':    ACCOUNT_NUMBER
    }
    export_request  = requests.post(API_HOST + EXPORT_ENDPOINT, cookies=login_request.cookies, data=export_payload, headers={'accept': 'application/json'})

    download_params = {
        'district':     export_request.json()['district'],
        'username':     export_request.json()['username'],
        'type':         export_request.json()['type'],
        'filename':     export_request.json()['filename']
    }
    download_request = requests.get(API_HOST + DOWNLOAD_ENDPOINT, cookies=login_request.cookies, params=download_params)

    csv_content = download_request.content.decode('utf-8')
    csv_reader = csv.reader(csv_content.splitlines(), delimiter=',')

    report = {
        'readings': {},
    }
    line_count = 0
    for row in csv_reader:
        if line_count >= CSV_ROW_OFFSET:
            if 'readings' not in report:
                report['readings'] = {}
            if 'timestamp' not in report:
                report['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            if 'last_reported' not in report:
                report['last_reported'] = row[CSV_COLUMN_LAST_DAY]
            if len(row[CSV_COLUMN_GALLONS]):
                report['readings'][row[CSV_COLUMN_TIMESTAMP]] = {
                    'cf':       row[CSV_COLUMN_CF],
                    'gal':      row[CSV_COLUMN_GALLONS],
                    'meter':    row[CSV_COLUMN_READING]
                }
        line_count += 1
    
    return report

def update_cache(report, filename):
    json_object = json.dumps(report, indent = 4)
    with open(pathlib.Path(__file__).parent / filename, 'w') as outfile:
        outfile.write(json_object)

def read_cache(filename):
    try:
        with open(pathlib.Path(__file__).parent / filename, 'r') as openfile:
            return json.load(openfile)
    except FileNotFoundError:
        return {}

def get_or_update_cached_water_report(reading_dt_str, filename, cache_age):
    hourly_water_usage_report = read_cache(filename)
    if 'timestamp' in hourly_water_usage_report:
        cache_timestamp = datetime.datetime.strptime(hourly_water_usage_report['timestamp'], '%Y-%m-%d %H:%M')
        if datetime.datetime.now() > cache_timestamp + datetime.timedelta(hours=cache_age):
            hourly_water_usage_report = get_hourly_water_usage_report(USERNAME, PASSWORD, ACCOUNT_NUMBER, DISTRICT)
            update_cache(hourly_water_usage_report, CACHE_FILE)
        elif reading_dt_str not in hourly_water_usage_report['readings']:
            hourly_water_usage_report = get_hourly_water_usage_report(USERNAME, PASSWORD, ACCOUNT_NUMBER, DISTRICT)
            update_cache(hourly_water_usage_report, CACHE_FILE)
    else:
        hourly_water_usage_report = get_hourly_water_usage_report(USERNAME, PASSWORD, ACCOUNT_NUMBER, DISTRICT)
        update_cache(hourly_water_usage_report, CACHE_FILE)
    return hourly_water_usage_report

def get_water_use_gal_reading(reading_dt):
    reading_dt_str = datetime.datetime(reading_dt.year, reading_dt.month, reading_dt.day, reading_dt.hour).strftime('%Y-%m-%d %H:%M')
    water_report = get_or_update_cached_water_report(reading_dt_str, CACHE_FILE, CACHE_EXPIRY_HOURS)
    for i in range(MISSING_READINGS):
        try:
            return water_report['readings'][reading_dt_str]
        except KeyError:
            reading_dt -= datetime.timedelta(hours=1)
            reading_dt_str = datetime.datetime(reading_dt.year, reading_dt.month, reading_dt.day, reading_dt.hour).strftime('%Y-%m-%d %H:%M')

reading_dt = datetime.datetime.now() - datetime.timedelta(days=1)
print(json.dumps(get_water_use_gal_reading(reading_dt)))
