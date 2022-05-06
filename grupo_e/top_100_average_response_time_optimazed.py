import itertools
import logging
import logging.config
import multiprocessing
import os
import time
from datetime import datetime
from functools import reduce
from os import path

from defusedxml.ElementTree import Et


def chunkify(iterable, number_of_chunks):
    """Return number of data required"""
    for i in range(0, len(iterable), number_of_chunks):
        yield iterable[i:i + number_of_chunks]


def get_post_question(data):
    """Return list of data posts questions"""
    try:
        score_xml = int(data.attrib['Score'])
        post_question_xml = int(data.attrib['PostTypeId'])
        id_post_xml = int(data.attrib['Id'])
        creation_date_xml = datetime.strptime(
            data.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')

        if score_xml and post_question_xml == 1 and id_post_xml and creation_date_xml:
            return {'id_post': id_post_xml,
                    'score': score_xml,
                    'creation_date_question': creation_date_xml}
    except KeyboardInterrupt:
        return None


def get_post_answers(data):
    """Return list of data posts answers"""
    try:
        post_question_xml = int(data.attrib['PostTypeId'])
        creation_date_xml = datetime.strptime(
            data.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')

        if post_question_xml == 2 and creation_date_xml:
            parent_id_xml = int(data.attrib['ParentId'])
            return {'creation_date_answer': creation_date_xml,
                    'parent_id': parent_id_xml}
    except KeyboardInterrupt:
        return None


def first_100_post_by_score(data):
    """Return a list of top 100 posts by score"""
    list_dicts = []
    for i in range(len(data)):
        for y in range(len(data[i])):
            list_dicts.append(data[i][y])

    # Order by score and omit negatives values
    dict_ordered = sorted(
        list_dicts, key=lambda e: (-e['score'], e['id_post']))  # reverse = True

    first_100 = list(dict_ordered)[:100]
    logger_console.debug('First 100 posts by score was returned successfully')
    return first_100


def join_questions_answers(data1, data2):
    """
    Return creation date question and answer into single list.
    Args:
        data1({dict}): {'id_post' : 0,
                        'score': 0,
                        'creation_date_question' : '%Y-%m-%dT%H:%M:%S.%f'}

        data2(list[{dict},...]): [{'creation_date_answers': 2009, 6, 28, 8, 14, 46, 627000,
                                  'parent_id': 0},{}...]

    Returns:
        List of dicts: Merge two dates questions and answers.
    """
    x = 0
    y = 0
    post_dates = []
    dict_join = {}
    for i in itertools.cycle(range(len(data1))):
        if data2[x][y]['parent_id'] == data1[i]['id_post']:
            dict_join = {'id': data1[i]['id_post'],
                         'creation_date_question': data1[i]['creation_date_question'],
                         'creation_date_answer': data2[x][y]['creation_date_answer']}
            post_dates.append(dict_join)

        if i == len(data1) - 1:
            y += 1
        if y == len(data2[x]) - 1:
            y = 0
            x += 1
            if x == len(data2):
                break
    logger_console.debug('Posts dates were returned successfully')
    return post_dates


def filter_answers_dates(a):
    """
    Find the first date when a posts was answer.
    Args:
       [ a (dict) : 'id': 0,
                    'creation_date_question' : '...',
                    'creation_date_answer' : '...'},{}...]
    """
    temp = []
    first_answer = []
    for i in range(len(a) - 1):
        if a[i]['id'] == a[i+1]['id']:
            dict_temp = dict(a[i].items())
            temp.append(dict_temp['creation_date_answer'])
        else:
            first_answer.append({'id': a[i]['id'],
                                 'first_response_date': max(temp),
                                 'creation_date_question': a[i]['creation_date_question']})
            temp = []
            continue
        if i == len(a) - 1:
            #a = a[i+1]
            break
    logger_console.debug(
        'Filter and returned first date of the answer succesfully')
    return first_answer


def response_time_post(data):
    """Return date difference"""
    return data['first_response_date'] - data['creation_date_question']


def mapper_questions(data):
    """Process data from chunkify()"""
    mapped = list(map(get_post_question, data))
    mapped = list(filter(None, mapped))
    return mapped


def mapper_answers(data):
    """Process data from chunkify()"""
    mapped = list(map(get_post_answers, data))
    mapped = list(filter(None, mapped))
    return mapped


if __name__ == '__main__':
    '''
    From the ranking of the top 0-100 by score,
    then get the average response of the posts.
    Requirements:
        - Id
        - Score
        - PostTypeId:
                    - 1 : Post question
                            - CreationDate
                    - 2 : Post answer
                            - ParentID
                            - CreationDate
    '''
    # Start time proccess
    start = time.process_time()

    # Logger paths
    log_file_path = path.join(path.dirname(
        path.abspath(__file__)), 'log.cfg')
    folder_path = path.join(path.dirname(
        path.abspath(__file__)), 'logs', '')
    logfilename = path.join(path.dirname(
        path.abspath(__file__)), 'logs', 'log_top100op.log')

    # Check if folder exist
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

    # Logger config file
    logging.config.fileConfig(log_file_path,
                              defaults={'logfilename': logfilename})
    logger_dev = logging.getLogger('file_log')
    logger_console = logging.getLogger(__name__)

    # Path XML
    parent_path = path.join(path.dirname(
        path.abspath(__file__)))
    # XML post file
    tree = Et.parse(parent_path + './/112010 Meta Stack Overflow//posts.xml')

    # Get the parent tag of the xml document
    root = tree.getroot()

    # Segment data
    data_chunk1 = chunkify(root, number_of_chunks=150)
    data_chunk2 = chunkify(root, number_of_chunks=150)

    # Multiprocessing.  - cpu_cores = num of cores
    cpu_cores = int(multiprocessing.cpu_count())

    with multiprocessing.Pool(cpu_cores) as p:
        # Map question and answers
        mapped_questions = list(p.map(mapper_questions, data_chunk1))
        mapped_questions = list(filter(None, mapped_questions))
        mapped_questions = list(first_100_post_by_score(mapped_questions))

        mapped_answers = list(p.map(mapper_answers, data_chunk2))
        mapped_answers = list(filter(None, mapped_answers))

        # Match by (id, parent_id) and return -> dates
        mapped_qa = join_questions_answers(mapped_questions,
                                           mapped_answers)

        # Order values by id.   -opt arg: reverse = True
        mapped_qa = sorted(mapped_qa, key=lambda e: (-e['id'], e['id']))

        # Find first date of answers
        mapped_qa = filter_answers_dates(mapped_qa)

        # Time to respond a question (date difference).
        mapped_qa = list(p.map(response_time_post, mapped_qa))

    # Reduce to obtain (average) of response
    average_reponses = reduce(lambda i, j: i + j, mapped_qa)/len(mapped_qa)

    # Save in log file
    logger_dev.info(
        f'Result average response time: {average_reponses}')

    # Obtain the run time and saved log file
    logger_dev.info(
        f'Num of cores: {cpu_cores}, Runtime: {str(time.process_time() - start)}s')
