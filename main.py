from distributed import Client
import re
import os


def mapper(file):
    word_map = []
    for line in file:
        line = line.lower()
        words = [word for word in re.split("[^a-z']+", line) if word]
        for word in words:
            word_map.append(f'({word},1)')
    return word_map

def sorter(seq):
    seq.sort()
    return seq


def reducer(mapped_words):
    last_word = None
    word_count = 0
    counted_words = []
    for line in mapped_words:
        line = line[1:-1]
        word, count = line.split(',')
        try:
            count = int(count)
        except:
            print()

        if word == last_word:
            word_count += count
        else:
            if last_word:
                counted_words.append(f'{last_word} - {word_count}')
            last_word = word
            word_count = count

    counted_words.append(f'{last_word} - {word_count}')
    return counted_words


def presenter(data):
    data_dict = {value.split(' - ')[0]: int(value.split(' - ')[1]) for value in data}
    data_dict = {k: v for k, v in sorted(data_dict.items(), key=lambda item: item[1], reverse=True)}
    data_list = [f'{k} - {v}' for k, v in data_dict.items()]
    return data_list[:100]

def file_reader(path):
    files = [file for file in os.listdir(path)]
    result = []
    for file in files:
        for line in open(path+file, encoding='ISO-8859-1'):
            result.append(line)
    return result

def main():
    file_content = file_reader('./books/')
    print('Files read')
    client = Client('tcp://192.168.1.40:8786')

    dsk = {
        'reader': file_content,
        'mapper': (mapper, 'reader'),
        'sorter': (sorter, 'mapper'),
        'reducer': (reducer, 'sorter'),
        'presenter': (presenter, 'reducer')
    }

    result = client.get(dsk, 'presenter')

    for word in result:
        print(word)




if __name__ == '__main__':
    main()
