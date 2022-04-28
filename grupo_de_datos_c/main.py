import time
from multiprocessing import Pool

from my_log import my_log
from task1 import top_10_accepted_answer
from task_2 import relationship


def main():
    with Pool(processes=8) as pool:
        result1 = pool.apply_async(top_10_accepted_answer)
        print(f'\n\ntop 10 accepted answer\n\n{result1.get()}', end='\n\n')
        result2 = pool.apply_async(relationship)
        print(
            f'\n\nThe total relationship between number of words in a post\
            and its number of responses is {result2.get()} words by \
                responses', end='\n\n')


if __name__ == '__main__':
    inicio = time.time()
    main()
    fin = time.time()
    logger = my_log(__name__)
    logger.debug(f'\nFinished in {(fin-inicio):.2f} seconds')
