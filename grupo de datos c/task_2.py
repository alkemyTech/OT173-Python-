import re
from functools import reduce
from xml.etree.ElementTree import Element

from load_file import chunkify, root_data
from my_log import my_log


def relationship_post_accepted_answer(data: Element) -> dict or None:
    """Returns the relationship between the post and the accepted answer

    Args:
        data (xml.etree.ElementTree.Element): post data

    Returns:
        dict:(id,words_in_post,answers_in_post)
    """
    try:
        AnswerCount = data.attrib['AnswerCount']
        body = data.attrib['Body']
        body = re.sub('((<(.+?)>|\"|\'))', '', body)
        body = re.findall(r'\w+', body)
        body_count = len(body)
        return {
            'words_in_post': int(body_count),
            'answers_in_post': int(AnswerCount)
            }
    except KeyError:
        return None


def mapped_relationship(chunk: list) -> list:
    """Processes the chunk of data and returns the relationship between the\
        post and the accepted answer
    Args:
        chunk (generator): chunk of data

    Returns:
        list: list of tuples (id, AcceptedAnswerId)
    """
    acepted = list(filter(
        None,
        list(map(relationship_post_accepted_answer, chunk))))
    return acepted


def reducer_relationship(tuple1, tuple2) -> dict:
    """Reduces chunks of data

    Args:
        dic1 (dict): (words_in_post, answers_in_post)

    Returns:
        dict: (words_in_post, answers_in_post)
    """

    tuple1 += tuple2
    return tuple1


def reducer_relationship_total(dic1: dict, dic2: dict) -> dict:
    """Reduces dict to total of words and answers

    Args:
        dic1 (dict): (words_in_post, answers_in_post)

    Returns:
        dict: (words_in_post, answers_in_post)
    """
    dic1['words_in_post'] += dic2['words_in_post']
    dic1['answers_in_post'] += dic2['answers_in_post']
    return dic1


def relationship_post_accepted_answer_total(dic_total: dict) -> float:
    """Returns the relationship between the post and the accepted answer"""
    relation = dic_total['words_in_post']/dic_total['answers_in_post']
    return round(relation, 2)


def relationship() -> float:
    """relationship between number of words in a post and its number\
        of responses"""
    logger = my_log(__name__)
    logger.info('Started getting relationship between number of words\
        in a post and its number of responses')
    root = root_data(logger)
    data_chunk = chunkify(root, 50)
    logger.info('Started processing chunk')
    mapped_data = list(map(mapped_relationship, data_chunk))
    logger.info('Finished processing chunk')
    logger.info('Started reducing relationship')
    reduced_data = reduce(reducer_relationship, mapped_data)
    reduced_data_total = reduce(reducer_relationship_total, reduced_data)
    logger.info('Finished reducing relationship')
    return relationship_post_accepted_answer_total(reduced_data_total)


if __name__ == '__main__':
    print(relationship())
