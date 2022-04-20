import logging
import logging.config
import logging.handlers
import os
from functools import reduce
from os import path
from task_1 import chunkify, mapped_chunks_top10, reducer_top10
from task_2 import chunkify, mapper_words_score, reducer
from task_3 import chunkify, mapper_dates, sum_dates

import defusedxml.ElementTree as ETree

""" In this tasks I had to use MapReduce for data´s Group A.
The tasks are:
1. Top 10 Tags with more accepted answers. I import the functions from task_1 file to resolve the task
2. Relation between number of words in a post and the score. I import the functions from task_2 file to resolve the task
3. Average time answer in posts. I import the functions from task_3 file to resolve the task
"""
# I configure the logs using the logging.cfg file
log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.cfg')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger("Grupo_Datos_A")
logger.info("running")

dir = os.path.dirname(__file__)
file_path = f'{dir}/posts.xml'
mytree = ETree.parse(file_path)
myroot = mytree.getroot()


# Task 1: Top 10 tags with more accepted answers
def top_10_tags(root):
    # Load data in chunks of 100 dates
    data_chunks = chunkify(root, 100)
    mapped_chunks = mapped_chunks_top10(data_chunks)
    reduced = reduce(reducer_top10, mapped_chunks)
    # Get top 10 tags with more accepted answers:
    tags_top_10 = reduced.most_common(10)
    logger.info("\t Top 10 tags with more accepted answer: ")
    for tag, count in tags_top_10:
        logger.info(f'\t Tag Name: {tag}, Accepted Answers: {count}')


# Task 2: Average score in relation with the number of words
def relation_score_words(root):
    # Load data in chunks of 100 dates
    data_chunks_2 = chunkify(root, 100)
    mapped_chunks_2 = list(map(mapper_words_score, data_chunks_2))
    mapped = list(filter(None, mapped_chunks_2))
    relationship = reducer(mapped)
    reduced_2 = reduce(reducer, relationship[0:1])
    try:
        logger.info(f'\t The Average score in relation with the number of words is: {reduced_2[0:1]}')
    except TypeError:
        logger.warning("The date obtained is not a float")


# Task 3: Average time to answer a post
def average_answer_time(root):
    # Load data in chunks of 100 dates
    data_chunks_3 = chunkify(root, 100)
    mapped_chunks_3 = list(map(mapper_dates, data_chunks_3))
    times = sum_dates(mapped_chunks_3)
    reduced = reduce(lambda count, l: count + len(l), mapped_chunks_3, 0)
    average_time = times/reduced
    try:
        logger.info(f'\t The average time to answer a post is: \n {average_time} days')
    except TypeError:
        logger.warning("The date is not a number")


if __name__ == '__main__':

    top_10_tags(myroot)
    relation_score_words(myroot)
    average_answer_time(myroot)
