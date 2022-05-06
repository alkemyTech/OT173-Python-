import logging
import logging.config
import os
import time
from collections import Counter
from functools import reduce
from os import path

from defusedxml.ElementTree import Et


def chunkify(iterable, number_of_chunks):
    """Return number of data required"""
    for i in range(0, len(iterable), number_of_chunks):
        yield iterable[i:i + number_of_chunks]


def mapper_dates(data):
    """Return data clean"""
    mapped = list(map(get_data, data))
    mapped = list(filter(None, mapped))
    return mapped


def get_data(data):
    """Return dictionary of responses and visits from XML"""
    try:
        responses_xml = int(data.attrib['AnswerCount'])
        visits_xml = int(data.attrib['ViewCount'])
        return {'responses': responses_xml, 'visits': visits_xml}
    except KeyboardInterrupt:
        return None


def sum_responses_visits(data1, data2):
    """Return total responses and visits"""
    if type(data1) is dict:
        data2.append(data1)
        list_merge = data2
    else:
        list_merge = data1 + data2

    counter = Counter()
    for d in list_merge:
        counter.update(d)
    result = dict(counter)
    return result


def relation_responses_visits(data):
    """Return relation beetwen responses and visits"""
    x, y = data.values()
    relation = y / x
    return relation


if __name__ == '__main__':
    '''
    Relationship between the number of responses and their visits.

    Requirements:
        - AnswerCount.
        - ViewCount.
    Details:
        1. Sum all values.
        2. Div values and obtain relation value.
    '''
    # Start time proccess
    start = time.process_time()

    # Logger paths
    log_file_path = path.join(path.dirname
                              (path.abspath(__file__)), 'log.cfg')
    folder_path = path.join(path.dirname
                            (path.abspath(__file__)), 'logs', '')
    logfilename = path.join(path.dirname
                            (path.abspath(__file__)), 'logs', 'log_relation_res_vis.log')

    # Check folder if exist
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

    # Logger config file
    logging.config.fileConfig(log_file_path,
                              defaults={'logfilename': logfilename})
    logger_dev = logging.getLogger('file_log')
    logger_console = logging.getLogger(__name__)

    # Path XML
    parent_path = path.join(path.dirname
                            (path.abspath(__file__)))
    # XML post file
    tree = Et.parse(parent_path + './/112010 Meta Stack Overflow//posts.xml')

    # Getting the parent tag of the xml document
    root = tree.getroot()

    # Segmented data
    data_chunk = chunkify(root, number_of_chunks=50)

    # Mappers
    mapped = list(map(mapper_dates, data_chunk))
    mapped = list(filter(None, mapped))
    mapped = reduce(sum_responses_visits, mapped)

    # Relation between responses and visits
    relation_value = relation_responses_visits(mapped)

    # Saved in log file
    logger_dev.info(
        f"Relation between visits and responses is : {relation_value}")

    # Obtain the run time and saved in log file
    logger_dev.info(
        f'Runtime: {str(time.process_time() - start)}s')
