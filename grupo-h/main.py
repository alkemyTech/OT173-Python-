import itertools
import logging
import logging.config
import multiprocessing
import os
import re
import time
from datetime import datetime
from functools import reduce
from typing import Counter

import defusedxml.ElementTree as Et

current_dir = os.getcwd()
data_dir = os.path.abspath(
    os.path.join(
        current_dir,
        "Stack Overflow 11-2010/",
        "112010 Meta Stack Overflow",
        "posts.xml"))
logger_dir = os.path.abspath(
    os.path.join(
        current_dir,
        "grupo-h",
        "logging.cfg"))

logging.config.fileConfig(logger_dir)
logger = logging.getLogger('DatosH')

cpu_cores = int(multiprocessing.cpu_count())


def data_generator(data, len_chunk):
    """Read data. Useful for large datasets
    Args:
        data: a root tree XML file
        len_chunk: lenght of equal chunks of data
    Return a generator
    """
    return (data[i:i + len_chunk] for i in range(0, len(data), len_chunk))


def split_type_post_words(data):
    return dict([[tipo_post, data[1].copy()] for tipo_post in data[0]])


def reduce_count_views(data1, data2):
    """Takes post with less views"""
    if data1[1] < data2[1]:
        return data1
    else:
        return data2


def reduce_counters(data1, data2):
    """Reduce counters per tag"""
    for key, value in data2.items():
        if key in data1.keys():
            data1[key].update(data2[key])
        else:
            data1.update({key: value})
    return data1


def get_post_type_and_words(data):
    """Extract post type and count words"""
    type_post = data.attrib['PostTypeId']
    body = data.attrib['Body']
    body = re.findall(
        '(?<!\\S)[A-Za-z]+(?!\\S)|(?<!\\S)[A-Za-z]+(?=:(?!\\S))',
        body)
    counter_words = Counter(body)
    return type_post, counter_words


def calculate_top_10(data):
    """Calculate Top 10 by word counter"""
    return data[0], data[1].most_common(10)


def get_views_post(data):
    """Get title and view count of posts. Delete zero-view posts to sort posts with almost one view"""
    try:
        view_count = int(data.attrib['ViewCount'])
        if view_count == 0:
            raise Exception
        title = data.attrib['Title']
    except KeyError:
        return None
    except Exception:
        return None
    return title, view_count


def get_score(data):
    """Get Score for only answers"""
    try:
        post_type_id = int(data.attrib['PostTypeId'])
        if post_type_id == 2:
            parent_id = data.attrib['ParentId']
        else:
            return None
        post_id = data.attrib['Id']
        score = int(data.attrib['Score'])
        created_at = datetime.strptime(
            data.attrib['CreationDate'],
            '%Y-%m-%dT%H:%M:%S.%f')
    except KeyError:
        return None
    return post_id, parent_id, score, post_type_id, created_at


def get_questions(data):
    """Get questions by PostTypeId"""
    try:
        if int(data.attrib['PostTypeId']) == 1:
            post_id = data.attrib['Id']
            created_at = datetime.strptime(
                data.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')
        else:
            raise Exception
    except KeyError:
        return None
    except Exception:
        return None
    return {'post_id': post_id,
            'question_created_at': created_at}


def mapper_task_1(data):
    map_views = list(map(get_views_post, data))
    map_views = list(filter(None, map_views))
    try:
        reduce_views = reduce(reduce_count_views, map_views)
    except TypeError:
        return None
    return reduce_views


def mapper_task_2(data):
    mapped_words = list(map(get_post_type_and_words, data))
    mapped_words = list(filter(None, mapped_words))
    words_per_type_post = list(map(split_type_post_words, mapped_words))
    try:
        reduced = reduce(reduce_counters, words_per_type_post)
    except BaseException:
        return None
    return reduced


def mapper_task_3(data):
    map_scores_answers = list(map(get_score, data))
    map_scores_answers = list(filter(None, map_scores_answers))
    return map_scores_answers


def answers_dict(data):
    return {'post_id': data[0],
            'parent_id': data[1],
            'score': data[2],
            'post_type_id': data[3],
            'created_at': data[4]}


def mapper_questions(data):
    map_questions = list(map(get_questions, data))
    map_questions = list(filter(None, map_questions))
    return map_questions


def questions_answers(data):
    """Join question datetime to answers by question ID to answer parent ID"""
    parent_id = data['parent_id']
    question_created_at = [
        row for row in mapped_questions if row['post_id'] == parent_id][0]['question_created_at']
    return {'post_id': data['post_id'],
            'parent_id': data['parent_id'],
            'score': data['score'],
            'post_type_id': data['post_type_id'],
            'question_created_at': question_created_at,
            'answer_date': data['created_at']}


def answer_time(data):
    """Time between answer and question"""
    time_diff = data['answer_date'] - data['question_created_at']
    return time_diff


start = time.time()
tree = Et.parse(data_dir)
root = tree.getroot()
len_chunk = 100

# Generate questions database out of __main__ to avoid NameError due to
# child processes
with multiprocessing.Pool(cpu_cores) as p:
    data_questions = data_generator(root, len_chunk)
    mapped_questions = list(p.map(mapper_questions, data_questions))
    mapped_questions = list(filter(None, mapped_questions))
    mapped_questions = list(itertools.chain(*mapped_questions))

if __name__ == '__main__':
    # Read data
    with multiprocessing.Pool(cpu_cores) as p:
        # Top 10 least viewed posts
        data_chunks_1 = data_generator(root, len_chunk)
        mapped = list(p.map(mapper_task_1, data_chunks_1))
        mapped = list(filter(None, mapped))
        mapped.sort(key=(lambda x: x[1]))
        top_10_post_least_viewed = mapped[0:10]

        # Top 10 words most mentioned in posts by type of post
        data_chunks_2 = data_generator(root, len_chunk)
        mapped = list(p.map(mapper_task_2, data_chunks_2))
        mapped = list(filter(None, mapped))
        reduced = reduce(reduce_counters, mapped)
        top_10_post_most_freq_per_type_post = dict(
            p.map(calculate_top_10, reduced.items()))

        # Average response time from the ranking of the first 300-400 by score
        data_chunks_3 = data_generator(root, len_chunk)
        mapped_answers = list(p.map(mapper_task_3, data_chunks_3))
        mapped_answers = list(filter(None, mapped_answers))
        mapped_answers = list(itertools.chain(*mapped_answers))
        mapped_answers = list(p.map(answers_dict, mapped_answers))
        mapped_answers.sort(key=(lambda x: x['score']), reverse=True)
        map_scores_answers_300 = mapped_answers[0:300]
        map_scores_answers_400 = mapped_answers[0:400]
        mapped_questions_answers_300 = list(
            p.map(questions_answers, map_scores_answers_300))
        mapped_questions_answers_400 = list(
            p.map(questions_answers, map_scores_answers_400))
        time_response_300 = list(
            p.map(
                answer_time,
                mapped_questions_answers_300))
        time_response_400 = list(
            p.map(
                answer_time,
                mapped_questions_answers_400))
        avg_time_response_300 = reduce(
            lambda x, y:
            x + y,
            time_response_300) / len(time_response_300)
        avg_time_response_400 = reduce(
            lambda x, y:
            x + y,
            time_response_400) / len(time_response_400)

    # Visualize outputs
    logger.info('----Top 10 least viewed posts:')
    for post in top_10_post_least_viewed:
        logger.info(post)
    logger.info(
        '----Top 10 words most mentioned in posts by type of post (1: Question, 2: Answer, 3: ?):')
    for type_post, count_words in top_10_post_most_freq_per_type_post.items():
        logger.info(type_post)
        for word in count_words:
            logger.info(f'  {word}')
    logger.info(
        f'----Average response time from the ranking of the first 300 by score: {avg_time_response_300}')
    logger.info(
        f'----Average response time from the ranking of the first 400 by score: {avg_time_response_400}')
    end = time.time()

    logger.info(f'----Execution time: {round(end - start, 2)}')
