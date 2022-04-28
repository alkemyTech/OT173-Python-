from functools import reduce
from xml.etree.ElementTree import Element

from load_file import chunkify, root_data
from my_log import my_log


def get_data_top_10_accepted_answer(data: Element):
    """Returns the data of the post in a tuple (id, AcceptedAnswerId) /
    if the post is accepted

    Args:
        data (xml.etree.ElementTree.Element): post data

    Returns:
        tuple: (id, AcceptedAnswerId)
    """

    try:
        acepted_answer = data.attrib['AcceptedAnswerId']
        ids = data.attrib['Id']
        return int(ids), int(acepted_answer)
    except KeyError:
        return None


def mapprocess_10_accepted_answer(chunk: list):
    """Processes the chunk of data
    Args:
        chunk (generator): chunk of data

    Returns:
        list: list of tuples (id, AcceptedAnswerId)
    """
    acepted = list(filter(
        None,
        list(map(get_data_top_10_accepted_answer, chunk))))
    return acepted


def reducer_top_10_accepted_answer(tuple1, tuple2):
    """Reduces the top 10 accepted answers

    Args:
        tuple1 (tuple): (id, AcceptedAnswerId)
        tuple2 (tuple): (id, AcceptedAnswerId)

    Returns:
        tuple: (id, AcceptedAnswerId)
    """
    tuple1 += tuple2
    return tuple1


def convert_to_dict(tuple_data):
    """Converts the tuple to a dictionary

    Args:
        tuple1 (tuple): (id, AcceptedAnswerId)
        tuple2 (tuple): (id, AcceptedAnswerId)

    Returns:
        dict: dictionary with the post id and the accepted answer
    """
    dict_from_list = {
        'post_id': tuple_data[0],
        'accepted_answers': tuple_data[1]
        }
    return dict_from_list


def top_10_accepted_answer() -> list:
    """Returns the top 10 accepted answers

    Returns:
        list: list of dictionaries with the post id and the accepted answer
    """
    logger = my_log(__name__)
    logger.info('Started getting top 10 accepted answers')

    root = root_data(logger)
    data_chunk = chunkify(root, 50)
    logger.info('Started processing chunk')
    mapped_data = list(map(mapprocess_10_accepted_answer, data_chunk))
    logger.info('Finished processing chunk')
    logger.info('Started reducing accepted answers')
    reduced_data = reduce(reducer_top_10_accepted_answer, mapped_data)
    logger.info('Finished reducing accepted answers')
    logger.info('Started sorting top 10 accepted answers')
    top_10_accepted_answer = sorted(
        reduced_data, key=lambda x: x[1],
        reverse=True)[:10]
    logger.info('Finished sorting top 10 accepted answers')
    logger.info('Started converting to dict')
    top_10_in_dict = list(map(convert_to_dict, top_10_accepted_answer))
    logger.info('Finished top 10 accepted answers')
    return top_10_in_dict


if __name__ == '__main__':
    top_10_accepted_answer()
