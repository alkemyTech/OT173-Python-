import logging
import logging.config
import os
import time
from defusedxml.ElementTree import ET
from collections import Counter
from datetime import datetime
from functools import reduce
from operator import add
from os import path


>> > et = parse(xmlfile)


def chunkify(iterable, number_of_chunks):
    """Return number of data required"""
    for i in range(0, len(iterable), number_of_chunks):
        yield iterable[i:i + number_of_chunks]


def get_dates(data):
    """Return posts question from XML"""
    dates_xml = data.attrib['CreationDate']
    post_type_xml = int(data.attrib['PostTypeId'])

    if dates_xml and post_type_xml == 1:
        dates_formated = datetime.strptime(
            dates_xml, '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d')
        dates_formated = datetime.strptime(dates_formated, '%Y-%m-%d').date()
        return dates_formated


def repeated_dates(data):
    """Return a counter type dictionary of posts dates"""
    counter_dates = dict(Counter(data))
    return counter_dates


def mapper_dates(data):
    """Return the list of dates that were filtered"""
    _dates = list(map(get_dates, data))
    _dates = list(filter(None, _dates))
    _dates = repeated_dates(_dates)
    return _dates


if __name__ == '__main__':
    '''
    Top 10 dates with the highest number of posts created.
    Requirements:
        - CreationDate.
        - PostTypeId 
    Details: 
        - Date format received: e.g. 2009-06-28T07:14:29.363
        - Date format python: %Y-%m-%dT%H:%M:%S.%f
    '''
    # Start the time proccess
    start = time.process_time()

    # Logger paths
    log_file_path = path.join(path.dirname
                             (path.abspath(__file__)), 'log.cfg')
    folder_path = path.join(path.dirname
                           (path.abspath(__file__)), 'logs', '')
    logfilename = path.join(path.dirname
                           (path.abspath(__file__)), 'logs', 'log_top10dates.log')

    # Check if exist folder
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

    # Logger config file
    logging.config.fileConfig(log_file_path, defaults={'logfilename': logfilename})
    logger_dev = logging.getLogger('file_log')
    logger_console = logging.getLogger(__name__)

    # Path XML
    parent_path = path.join(path.dirname(path.abspath(__file__)))
    
    # XML post file
    tree = ET.parse(parent_path + './/112010 Meta Stack Overflow//posts.xml')

    # Getting the parent tag of the xml document
    root = tree.getroot()

    # Segmented data
    data_chunk = chunkify(root, number_of_chunks=100)

    # Top 10 dates with the highest number of posts created.
    mapped = list(map(mapper_dates, data_chunk))
    reduced = reduce(add, (Counter(dict(x)) for x in mapped))
    top_10 = reduced.most_common(10)

    # Convert tupple to string
    top_10 = [(str(x), str(y)) for x, y in top_10]

    # Print console top 10 dates with more posts
    logger_console.info(top_10)

    # Obtain the Runtime and save log file
    logger_dev.info(f'Runtime: {str(time.process_time() - start)}s')
