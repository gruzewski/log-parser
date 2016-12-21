from __future__ import absolute_import

import re
from lib.papertrail import PapertrailProvider


# Example line patterns
line_pattern = re.compile('.*\[(?P<log_date>\d{2,4}-\d{2}-\d{2}) (?P<log_time>\d{2}:\d{2}:\d{2}).*?\] (?P<log_message>.*)')

task_message_pattern = re.compile('(?P<log_task_name>.*)\[(?P<log_task_id>[a-zA-Z0-9-]+)\]: Asynchronous charge (?P<log_task_status>\w+). UserId: (?P<log_user_id>\w+), Amount: (?P<log_amount>\d+), reference: (?P<log_reference>[a-zA-Z0-9-]+)')

http_message_pattern = re.compile('.*?\[(?P<http_date>\d{2}\/\w+\/\d{2,4}):(?P<http_time>\d{2}:\d{2}:\d{2}).*\] \"(?P<http_method>.*?)\" \d{3} .*-\" (?P<http_duration>\d+) \"(?P<http_reference>[a-zA-Z0-9-]+)\"')

async = []
async_time = []
sync_time = []
endpoint_time = {}

parser = PapertrailProvider(token='<token>')

# To Do:
#  - provide function that accepts filtering function as parameter and yields lines that matches the filter
#  - write documentation
#  - add scheduler
#  - dockerise
#  - remove below (test only)

# Below is example how to process logs to get only interesting fields
#
# for line in parser.get_from_s3(start_date='2016-12-05 14:20:00', end_date='2016-12-05 15:00:00', search_pattern=''):
#     macz_http = http_message_pattern.match(line)
#     mach_line = line_pattern.match(line)
#
#     if macz_http:
#         endpoint_time[macz_http.group('http_reference')] = macz_http.group('http_duration')
#     elif mach_line:
#         macz_2 = task_message_pattern.match(mach_line.group('log_message'))
#         if macz_2:
#             ref = macz_2.group('log_reference')
#             if ref in endpoint_time:
#                 async_time.append(endpoint_time.pop(ref))
#                 print(ref)
#                 print(line)
#             else:
#                 print('Missing: '.format(line))


for line in parser.get_from_s3(start_date='2016-12-02 14:10:57', end_date='2016-12-02 14:20:57', search_pattern='4a5ed505-fd88-4501-8d16-0ba1bf7da0e6'):
    print(line)

# for t_async in async_time:
#     print(t_async)