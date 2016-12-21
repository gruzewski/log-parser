import boto3
import lib.utils as utils
from moto import mock_s3
from tempfile import NamedTemporaryFile
import unittest


class TestUtils(unittest.TestCase):
    start_date = '2016-11-05 12:10:00'
    end_date = '2016-11-05 15:43:00'
    short_end_date = '2016-11-05 12:20:00'
    log_date_list = ['2016-11-05-12',
                     '2016-11-05-13',
                     '2016-11-05-14',
                     '2016-11-05-15'
                     ]
    epoch_list = [{'start': 1476572401.0, 'end': 1476576001.0},
                  {'start': 1476576001.0, 'end': 1476579601.0},
                  {'start': 1476579601.0, 'end': 1476580201}
                  ]

    def test_get_epoch_list_for_timestamp(self):
        self.assertListEqual(utils.get_epoch_intervals('2016-10-16 00:00:01', '2016-10-16 02:10:01', '1H'), self.epoch_list)

    def test_get_epoch_list_for_short_interval(self):
        self.assertListEqual(utils.get_epoch_intervals(self.start_date,
                                                       self.short_end_date,
                                                       '15min'),
                             [{'start': 1478347800.0, 'end': 1478348400.0}])

    def test_get_time_list_for_timestamp(self):
        self.assertListEqual(utils.get_time_intervals(self.start_date, self.end_date), self.log_date_list)

    def test_start_date_cannot_be_after_end_date(self):
        with self.assertRaises(AssertionError):
            utils.get_time_intervals('2016-11-05 17:10:00', self.end_date)

    @mock_s3
    def test_get_s3_log(self):
        s3bucket = 'log'
        search_pattern = 'Test 123\n'
        log_filename = '2016-11-11-10.log'
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=s3bucket)

        with NamedTemporaryFile(mode='w') as tmp:
            tmp.write(search_pattern)
            tmp.write('Noise')
            tmp.flush()
            s3.upload_file(tmp.name, s3bucket, log_filename)

        result = [line for line in utils.get_log_from_s3('s3://{}/{}'.format(s3bucket, log_filename), search_pattern)]

        self.assertEqual(result[0], search_pattern)
        self.assertEqual(len(result), 1)
