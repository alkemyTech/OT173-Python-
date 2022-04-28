import os
import xml.etree.ElementTree as ET


base_path = os.path.dirname(__file__)


def chunkify(iterable: ET.Element, chunk_size: int):
    """Splits the data into chunks of size len_of_chunk

    Args:
        iterable (xml.etree.ElementTree.Element): data to be split
        len_of_chunk (int): size of the chunk

    Yields:
        list: chunk of data
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]


def read_xml(file_path: str, logger):
    """Reads the xml file and returns the root element

    Args:
        file_path (str): path to the xml file

    Raises:
        FileNotFoundError: if the file does not exist

    Returns:
        ET.Element: root element
    """

    try:
        tree = ET.parse(file_path)
        logger.info('File loaded successfully')
    except FileNotFoundError as e:
        logger.error('File not found')
        raise e
    return tree.getroot()


def root_data(logger):
    """root data"""
    return read_xml(os.path.join(
        base_path, '112010 Meta Stack Overflow', 'posts.xml'), logger)
