from __future__ import absolute_import

from datetime import datetime, timedelta
from lib.logprovider import LogProvider
import lib.utils as utils
import requests
from time import mktime, strptime


class PapertrailProvider(LogProvider):
    API_ENDPOINT = 'https://papertrailapp.com/api/v1'
    API_SEARCH_URI = 'events/search.json'
    API_SEARCH_ENDPOINT = API_ENDPOINT + '/' + API_SEARCH_URI
    BUCKET = '<your_bucket>'
    FILENAME_FORMAT = '%Y-%m-%d-%H'
    FOLDER_PREFIX = '<folder_prefix>'
    FILE_SUFFIX = 'tsv.gz'
    S3_LOG_DELAY = 2

    def __init__(self, token=None, format='json'):
        self.token = token
        self.format = format

    def _payload(self, tmin, query='', group_name=''):
        return {
            'format': self.format,
            'min_time': tmin,
            'q': query,
            'group_name': group_name
        }

    def _headers(self):
        return {'X-Papertrail-Token': self.token}

    def get_logs(self, start_date, end_date, search_pattern='', group_name=''):
        time_delta = timedelta(hours=self.S3_LOG_DELAY)
        new_start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        new_end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        now = datetime.now()

        if new_end_date + time_delta >= now:
            if new_start_date + time_delta > now:
                for line in self.get_from_cloud(start_date=new_start_date, end_date=new_end_date, search_patter=search_pattern, group_name=group_name):
                    yield line
            else:
                for line in self.get_from_cloud(start_date=new_end_date - time_delta, end_date=new_end_date, search_patter=search_pattern, group_name=group_name):
                    yield line

                for line in self.get_from_s3(start_date=start_date, end_date=end_date, search_pattern=search_pattern):
                    yield line
        else:
            for line in self.get_from_s3(start_date=start_date, end_date=end_date, search_pattern=search_pattern):
                yield line

    def get_from_cloud(self, start_date, end_date, search_patter='', group_name=''):

        epoch_last = int(mktime(strptime(start_date, '%Y-%m-%d %H:%M:%S')))
        epoch_end = int(mktime(strptime(end_date, '%Y-%m-%d %H:%M:%S')))

        # Papertrail doesn't allow both min_time and max_time in one call.
        while epoch_last < epoch_end:
            r = requests.get(self.API_SEARCH_ENDPOINT,
                             headers=self._headers(),
                             params=self._payload(tmin=epoch_last, query=search_patter, group_name=group_name)
                             )

            for line in r.json().get('events'):
                yield line

            epoch_last = int(mktime(strptime(r.json().get('max_time_at')[:19], '%Y-%m-%dT%H:%M:%S')))

    def get_from_s3(self, start_date, end_date, search_pattern=''):

        for ddate in utils.get_time_intervals(start_date, end_date, interval='1H', format=self.FILENAME_FORMAT):
            folder = self.FOLDER_PREFIX + ddate[:-3] # Remove minutes
            filename = ddate + '.' + self.FILE_SUFFIX
            url = 's3://' + self.BUCKET + '/' + folder + '/' + filename
            for line in utils.get_log_from_s3(url, search_pattern):
                yield line

# To Do:
#  - environment variables
#  - remove below (test only)
# parser = PapertrailProvider(token='<your_token_here>')
# with open('logs.txt', mode='w') as log_file:
#     for line in parser.get_from_s3(start_date='2016-11-01 00:00:01',
#                                    end_date='2016-12-07 23:59:59',
#                                    search_pattern='deadlock detected'):
#         log_file.write(line)
#
#result = parser.get_from_s3(start_date='2016-11-13 16:20:00', end_date='2016-11-13 18:34:00', search_pattern='<id>')

#print(len(result))
#print(result)