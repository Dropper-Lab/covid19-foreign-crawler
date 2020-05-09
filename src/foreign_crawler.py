"""
version : v1.0.6

MIT License

Copyright (c) 2020 Dropper Lab

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
from logging.handlers import RotatingFileHandler

import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime

import json
import time

import pymysql

import mysql_foreign_property
import foreign_property
import mail_sender

logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/foreign_crawler.log', maxBytes=1024 * 1024 * 1024 * 9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info('every package loaded and start logging')

logger.info(
    'mysql_foreign_property.hostname=' + str(mysql_foreign_property.hostname) + ' | mysql_foreign_property.user=' + str(
        mysql_foreign_property.user) + ' | mysql_foreign_property.password=' + str(
        mysql_foreign_property.password) + ' | mysql_foreign_property.database=' + str(
        mysql_foreign_property.database) + ' | mysql_foreign_property.charset=' + str(mysql_foreign_property.charset))
logger.info('foreign_property.country_dictionary=' + str(foreign_property.country_dictionary))


def insert_result(uid, data_list):
    logger.info('insert_result: function started')
    connection = pymysql.connect(host=mysql_foreign_property.hostname, user=mysql_foreign_property.user,
                                 password=mysql_foreign_property.password, db=mysql_foreign_property.database,
                                 charset=mysql_foreign_property.charset)
    logger.info('insert_result: database connection opened')
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger.info('insert_result: database cursor created')

    for data in data_list[1:]:
        cursor.execute(
            f"insert into foreign_{data['country']} values({uid}, {data_list[0]}, {data['certified']}, {data['dead']});")
        logger.info('insert_result: foreign_' + str(data['country']) + ' data inserted | uid=' + str(
            uid) + ' | data_list[0]' + str(data_list[0]) + ' | data=' + str(data))

    connection.commit()
    logger.info('insert_result: database connection commited')
    connection.close()
    logger.info('insert_result: database connection closed')

    logger.info('insert_result: function ended')


def dump_result(uid, data):
    logger.info('dump_result: function started')

    with open('./foreign-data/k_covid19_foreign_' + str(uid) + '.json', 'w') as json_file:
        json.dump(data, json_file)
    logger.info('dump_result: data dumped in foreign-data/k_covid19_foreign_' + str(uid) + '.json | data=' + str(data))

    logger.info('dump_result: function ended')


def get_foreign_data(target='', current_timestamp=0):
    logger.info('get_foreign_data: function started | target=' + target)

    downloaded_html = urlopen(target)
    logger.info('get_foreign_data: html downloaded')
    beautifulsoup_object = BeautifulSoup(downloaded_html, 'html.parser')
    logger.info('get_foreign_data: html parsed to beautifulsoup object')

    announced_time = ['2020',
                      re.findall('([0-9]+)[.]', beautifulsoup_object.findAll('p', class_='s_descript')[0].text)[0],
                      re.findall('[.]([0-9]+)', beautifulsoup_object.findAll('p', class_='s_descript')[0].text)[0],
                      re.findall('([0-9]+)시', beautifulsoup_object.findAll('p', class_='s_descript')[0].text)[0]]
    logger.info('get_foreign_data: get announced time | announced_time=' + str(announced_time))

    datetime_object = datetime.datetime.strptime(str(announced_time), "['%Y', '%m', '%d', '%H']")
    logger.info('get_foreign_data: convert announced time to datetime object | datetime_object=' + str(datetime_object))
    announced_time_unix = int(time.mktime(datetime_object.timetuple())) - 32400
    logger.info(
        'get_foreign_data: convert datetime object to unix time | announced_time_unix=' + str(announced_time_unix))

    raw_table = beautifulsoup_object.findAll('tbody')
    logger.info('get_foreign_data: table picked out | raw_table=' + str(raw_table))
    raw_table_beautifulsoup_object = BeautifulSoup(str(raw_table[0]), 'html.parser')
    logger.info('get_foreign_data: convert raw table to beautifulsoup object | raw_table_beautifulsoup_object=' + str(
        raw_table_beautifulsoup_object))
    table_data_rows = raw_table_beautifulsoup_object.findAll('tr')
    logger.info('get_foreign_data: export table data from raw_table_beautifulsoup_object | table_data_rows=' + str(
        table_data_rows))
    table_data_rows.reverse()
    logger.info('get_foreign_data: reverse exported data | table_data_rows=' + str(table_data_rows))

    foreign_data_list = [announced_time_unix]
    logger.info('get_foreign_data: declare foreign_data_list | foreign_data_list=' + str(foreign_data_list))

    convert_error_list = [0]
    database_error_list = [0]
    dictionary_error_list = [0]

    report_message = '* Dropper API Foreign Crawling Report *\n\n\n'
    report_level = 0

    index_no = 0

    try:
        table_data = table_data_rows[0]
        try:
            table_data_beautifulsoup_object = BeautifulSoup(str(table_data), 'html.parser')
            logger.info(
                'get_foreign_data: convert table_data to beautifulsoup object | table_data_beautifulsoup_object=' + str(
                    table_data_beautifulsoup_object))
            try:
                country = table_data_beautifulsoup_object.findAll('th')[0].text
                logger.info('get_foreign_data: extracting country from table data | country=' + str(country))
                try:
                    certified = re.sub('[,명]', '',
                                       re.sub('\(사망[  ][0-9,]+\)', '',
                                              table_data_beautifulsoup_object.findAll('td')[0].text))
                    logger.info('get_foreign_data: extracting certified from table data | certified=' + str(certified))
                    dead = re.findall('\(사망[  ]([0-9,]+)\)', table_data_beautifulsoup_object.findAll('td')[0].text)
                    logger.info('get_foreign_data: extracting dead from table data | country=' + str(dead))

                    # print('|' + foreign_property.country_dictionary[re.sub('[  ]', '', country)] + '|' + country + '|')
                    #  print('[\'foreign_' + foreign_property.country_dictionary[re.sub('[  ]', '', country)], end='\'')

                    foreign_data = {
                        'country': foreign_property.country_dictionary[
                            re.sub('[  ]', '', re.sub('[가-힣]^', '', country))],
                        'certified': int(certified),
                        'dead': int(re.sub('[,명]', '', dead[0])) if dead != [] else 0
                    }
                    logger.info('get_foreign_data: declare foreign data | foreign_data=' + str(foreign_data))

                    foreign_data_list.append(foreign_data)
                    logger.info(
                        'get_foreign_data: put foreign data into foreign data list | foreign_data_list=' + str(
                            foreign_data_list))
                except Exception as ex:
                    if report_level < 1:
                        report_level = 1
                    dictionary_error_list[0] = 1
                    dictionary_error_list.append([ex, table_data])
                    logger.info('get_foreign_data: unregistered country name was found | ex=' + str(
                        ex) + ' | dictionary_error_list=' + str(dictionary_error_list))
            except Exception as ex:
                if report_level < 2:
                    report_level = 2
                database_error_list[0] = 1
                database_error_list.append([ex, index_no])
                logger.info(
                    'get_foreign_data: cannot extract country from table data | ex=' + str(ex) + ' | index_no=' + str(
                        index_no))
        except Exception as ex:
            if report_level < 2:
                report_level = 2
            convert_error_list[0] = 1
            convert_error_list.append([ex, table_data])
            logger.info('get_foreign_data: cannot convert table_data to beautifulsoup object | ex=' + str(
                ex) + ' | table_data=' + str(table_data))
        for table_data, index_no in zip(table_data_rows[1:], range(1, len(table_data_rows))):
            try:
                table_data_beautifulsoup_object = BeautifulSoup(str(table_data), 'html.parser')
                logger.info(
                    'get_foreign_data: convert table_data to beautifulsoup object | table_data_beautifulsoup_object=' + str(
                        table_data_beautifulsoup_object))
                try:
                    country = table_data_beautifulsoup_object.findAll('td')[0].text
                    logger.info('get_foreign_data: extracting country from table data | country=' + str(country))
                    try:
                        certified = re.sub('[,명]', '',
                                           re.sub('\(사망[  ][0-9,]+\)', '',
                                                  table_data_beautifulsoup_object.findAll('td')[1].text))
                        logger.info(
                            'get_foreign_data: extracting certified from table data | certified=' + str(certified))
                        dead = re.findall('\(사망[  ]([0-9,]+)\)', table_data_beautifulsoup_object.findAll('td')[1].text)
                        logger.info('get_foreign_data: extracting dead from table data | country=' + str(dead))

                        # print('|' + foreign_property.country_dictionary[re.sub('[  ]', '', country)] + '|' + country + '|')
                        #  print(',\n\'foreign_' + foreign_property.country_dictionary[re.sub('[  ]', '', country)], end='\'')

                        foreign_data = {
                            'country': foreign_property.country_dictionary[
                                re.sub('[  ]', '', re.sub('[가-힣]^', '', country))],
                            'certified': int(certified),
                            'dead': int(re.sub('[,명]', '', dead[0])) if dead != [] else 0
                        }
                        logger.info('get_foreign_data: declare foreign data | foreign_data=' + str(foreign_data))

                        foreign_data_list.append(foreign_data)
                        logger.info(
                            'get_foreign_data: put foreign data into foreign data list | foreign_data_list=' + str(
                                foreign_data_list))
                    except Exception as ex:
                        if report_level < 1:
                            report_level = 1
                        dictionary_error_list[0] = 1
                        dictionary_error_list.append([ex, table_data])
                        logger.info('get_foreign_data: unregistered country name was found | ex=' + str(
                            ex) + ' | dictionary_error_list=' + str(dictionary_error_list))
                except Exception as ex:
                    if report_level < 2:
                        report_level = 2
                    database_error_list[0] = 1
                    database_error_list.append([ex, index_no])
                    logger.info('get_foreign_data: cannot extract country from table data | ex=' + str(
                        ex) + ' | index_no=' + str(index_no))
            except Exception as ex:
                if report_level < 2:
                    report_level = 2
                convert_error_list[0] = 1
                convert_error_list.append([ex, table_data])
                logger.info('get_foreign_data: cannot convert table_data to beautifulsoup object | ex=' + str(
                    ex) + ' | table_data=' + str(table_data))
    except Exception as ex:
        if report_level < 3:
            report_level = 3
        logger.info('get_foreign_data: table_data_rows is empty | ex=' + str(ex))
        report_message += '- FATAL: table_data_rows is empty -\n\n\n'
        report_message += str(ex) + '\n'
        report_message += '\n'
        report_message += '\nThis report is about table_data_rows ' + str(table_data_rows)
        report_message += '\n'
        report_message += '\n\n\n\n\n'

    #  print('],')

    if convert_error_list[0] == 1:
        report_message += '- ERROR: cannot convert table_data to beautifulsoup object -\n\n\n'
        for error in convert_error_list[1:]:
            report_message += '---------------------------\n'
            report_message += f"{error[0]}\n\ntable_data:\n{error[1]}\n"
        report_message += '---------------------------\n'
        report_message += '\n\n\n\n\n'

    if database_error_list[0] == 1:
        report_message += '- ERROR: cannot extract country from table data -\n\n\n'
        for error in database_error_list[1:]:
            report_message += '---------------------------\n'
            report_message += f"{error[0]}\n\nindex_no:\n{error[1]}\n"
        report_message += '---------------------------\n'
        report_message += '\n\n\n\n\n'

    if dictionary_error_list[0] == 1:
        report_message += '- WARN: unregistered country name was found -\n\n\n'
        for error in dictionary_error_list[1:]:
            report_message += '---------------------------\n'
            report_message += f"{error[0]}\n\ncountry_name:\n{error[1]}\n"
        report_message += '---------------------------\n'
        report_message += '\n\n\n\n\n'

    if report_level < 2:
        report_message += 'Crawling finished successfully\n'
        report_message += '\nThis report is based on (Unix Time)' + str(int(current_timestamp))
        if report_level == 0:
            mail_sender.send_mail(
                subject=f'[Dropper API](foreign_crawler) INFO: task report',
                message=report_message)
        elif report_level == 1:
            mail_sender.send_mail(
                subject=f'[Dropper API](foreign_crawler) WARN: task report',
                message=report_message)
    elif report_level == 2:
        report_message += 'Some error occurred while crawling\n'
        report_message += '\nThis report is based on (Unix Time)' + str(int(current_timestamp))
        mail_sender.send_mail(
            subject=f'[Dropper API](foreign_crawler) ERROR: task report',
            message=report_message)
    else:
        report_message += 'Fatal error occurred while crawling\n'
        report_message += '\nThis report is based on (Unix Time)' + str(int(current_timestamp))
        mail_sender.send_mail(
            subject=f'[Dropper API](foreign_crawler) FATAL: task report',
            message=report_message)

    logger.info('get_foreign_data: function ended | foreign_data_list=' + str(foreign_data_list))
    return foreign_data_list


if __name__ == '__main__':
    logger.info('start foreign_crawler.py')

    timestamp = int(time.time())
    logger.info('recorded a time stamp | timestamp=' + str(timestamp))

    result = get_foreign_data(target='http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=14',
                              current_timestamp=timestamp)
    logger.info('get result | result=' + str(result))

    dump_result(timestamp, result)
    logger.info('dump result | timestamp=' + str(timestamp) + ' | result=' + str(result))
    insert_result(timestamp, result)
    logger.info('insert result | timestamp=' + str(timestamp) + ' | result=' + str(result))

    logger.info('end foreign_crawler.py')
