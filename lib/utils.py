import logging
import pandas as pd
import re
import smart_open
from time import mktime, strptime

# HACK: https://github.com/boto/boto/issues/2836
import ssl
if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context


def get_epoch_intervals(start_date, end_date, interval='15min'):
    date_format = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{1,2}')
    assert date_format.match(start_date) and date_format.match(end_date)

    start_date_formated = int(mktime(strptime(start_date, '%Y-%m-%d %H:%M:%S')))
    end_date_formated = int(mktime(strptime(end_date, '%Y-%m-%d %H:%M:%S')))

    t_ranges = pd.date_range(start=pd.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S'),
                             end=pd.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S'),
                             freq=interval,
                             closed='left'
                             )

    epochs = [{
                  'start': mktime(t_ranges[i].timetuple()),
                  'end': mktime(t_ranges[i+1].timetuple())
               } for i in range(len(t_ranges)-1)]

    # data_ranges doesn't include the last range if it is shorten than interval
    if epochs:
        if epochs[-1].get('end') < end_date_formated:
            epochs.append({'start': epochs[-1].get('end'), 'end': end_date_formated})
    else:
        epochs.append({'start': start_date_formated, 'end': end_date_formated})

    return epochs


def get_time_intervals(start_date, end_date, interval='1H', format='%Y-%m-%d-%H'):
    date_format = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{1,2}')
    assert date_format.search(start_date) and date_format.match(end_date)

    new_start_date = pd.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    new_end_date = pd.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    assert new_start_date < new_end_date

    date_ranges = pd.date_range(start=new_start_date, end=new_end_date, freq=interval)

    return ['{}'.format(date.strftime(format)) for date in date_ranges]


def get_log_from_s3(file_url, search_pattern=None):
    try:
        if search_pattern:
            # TODO: search pattern as a list
            search = re.compile(search_pattern)
            for line in smart_open.smart_open(file_url, profile_name='y2k-prod'):
                string_line = line.decode(encoding='UTF-8')
                if search.findall(string_line):
                    yield string_line
        else:
            for line in smart_open.smart_open(file_url):
                yield line.decode(encoding='UTF-8')

    except KeyError:
        logging.warning('File {} doesnt exist.'.format(file_url))
