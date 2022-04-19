import logging.config
import re
from collections import Counter
from datetime import datetime

def chunkify(iterable, len_chunk):
    for i in range(0, len(iterable), len_chunk):
        yield iterable[i:i + len_chunk]


def dates_time(data):
    try:
        creation_date = data.attrib['CreationDate']
    except KeyError:
        return
    last_edit = data.attrib['LastActivityDate']
    creation_date = creation_date.replace("T", " ")
    last_edit = last_edit.replace("T", " ")
    creation_date = datetime.strptime(creation_date, "%Y-%m-%d %H:%M:%S.%f")
    last_edit = datetime.strptime(last_edit, "%Y-%m-%d %H:%M:%S.%f")
    res = abs((creation_date-last_edit).days)
    return res


def mapper_dates(data):
    mapped_dates = list(map(dates_time, data))
    return mapped_dates


def sum_dates(data):
    data = [[a/2 for a in dates if a] for dates in data]
    data = [sum(i) for i in zip(*data)]
    return sum(data)
