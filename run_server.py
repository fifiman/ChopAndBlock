from server import Server
import logging
import sys


def map(key, values):
    for word in values.split():
        yield (word, 1)


def reduce(key, values):
    return sum(values)


if __name__ == '__main__':

    server = Server()

    server.map = map
    server.reduce = reduce

    fileName = "text.txt"
    data = ""
    with open(fileName) as f:
        data = [f.read().strip()]

    server.data = dict(enumerate(data))

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    outputFileName = "words.txt"
    buffer = ''

    result = server.run_server()

    for word, count in result.items():
        buffer += word + ' ' + str(count) + '\n'

    with open(outputFileName, 'w') as f:
        f.write(buffer)
