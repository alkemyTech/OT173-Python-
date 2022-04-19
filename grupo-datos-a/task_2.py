import logging.config
import re
from collections import Counter
from functools import reduce

def chunkify(iterable, len_chunk):
    for i in range(0, len(iterable), len_chunk):
        yield iterable[i:i + len_chunk]


def scores(data):
    try:
        word = data.attrib['Body']
    except KeyError:
        return
    word = re.findall("<(?:\"[^\"]*\"['\"]*|'[^']*'['\"]*|[^'\">])+>",
                      word)
    score = data.attrib['Score']
    # I transform de variable score into int to find its relationship
    score = int(score)
    word_counter = Counter(word)
    word_counter = sum(word_counter.values())
    return score, word_counter


def reducer(mapped_list):
    return [[a / b for a, b in data if b] for data in mapped_list]


def mapper_words_score(data):
    mapped_scores = list(map(scores, data))
    mapped_scores = list(filter(None, mapped_scores))
    return mapped_scores