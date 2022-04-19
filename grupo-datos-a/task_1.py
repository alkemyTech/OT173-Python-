import logging.config
import re
from collections import Counter
from datetime import datetime

def chunkify(iterable, len_chunk):
    for i in range(0, len(iterable), len_chunk):
        yield iterable[i:i + len_chunk]


###Create a list with Tags and accepted answers
def tags_and_ans(data_chunk):
    tags = []
    acc_ans_id = []
    for data in data_chunk:
        tags.append(data.attrib.get('Tags'))
        acc_ans_id.append(data.attrib.get('AcceptedAnswerId'))

    return tags, acc_ans_id


#Create one list with Tags and accepted answers
def tags_acc_answ(data):
    tags_with_acc_ans = []
    data = zip(data[0], data[1])

    for tags, ans_id in data:
        if ans_id is not None:
            tags_with_acc_ans.append(tags)

    return tags_with_acc_ans



def count_tags(tags):
    counter = Counter()

    char_replacement = {'<': ' ', '>': ' '}

    for tag_string in tags:
        for key, value in char_replacement.items():
            tag_string = tag_string.replace(key, value)
        counter.update(Counter(tag_string.split()))

    return counter


#Create a map that contains the tags with accepted answers for the different chucks
def mapped_chunks_top10(chunks):
    mapped = map(tags_and_ans, chunks)
    mapped = map(tags_acc_answ, mapped)
    mapped = map(count_tags, mapped)
    return mapped


#Returns the accepted answers for Tags for Chuck
def reducer_top10(list_of_dict_1, list_of_dict_2):
    return list_of_dict_1 + list_of_dict_2
