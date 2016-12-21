import boto3
from datetime import datetime
from gzip import GzipFile
from freezegun import freeze_time
from mock import patch, Mock
from moto import mock_s3
from lib.papertrail import PapertrailProvider
from tempfile import NamedTemporaryFile
import unittest


class TestUtils(unittest.TestCase):
    start_date = '2016-11-05 12:10:00'
    end_date = '2016-11-05 15:43:00'
    log_date_list = ['2016-11-05-12',
                     '2016-11-05-13',
                     '2016-11-05-14',
                     '2016-11-05-15'
                     ]

    @mock_s3
    def test_get_log_from_s3(self):
        search_pattern = 'Test 123\n'

        papertrail = PapertrailProvider()

        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=papertrail.BUCKET)

        for log_file in self.log_date_list:
            with NamedTemporaryFile(mode='w+b', prefix=log_file, suffix='.tsv.gz') as tmp:
                gzfile = GzipFile(mode='wb', fileobj=tmp)
                gzfile.write(b'Noise r4nd0m\n')
                gzfile.write(search_pattern.encode(encoding='UTF-8'))
                gzfile.write(b'Noise Test 132 *\n')
                gzfile.flush()
                s3.upload_file(tmp.name, papertrail.BUCKET, '{}{}/{}.tsv.gz'.format(papertrail.FOLDER_PREFIX, log_file[:-3], log_file))

        result = [line.strip() for line in papertrail.get_from_s3(self.start_date, self.end_date, search_pattern)]

        self.assertListEqual(result, ['Test 123', 'Test 123', 'Test 123', 'Test 123'])

    @patch.object(PapertrailProvider, 'get_from_cloud')
    @patch.object(PapertrailProvider, 'get_from_s3')
    def test_get_log_more_than_two_hours_ago(self, mock_s3_method, mock_cloud_method):
        papertrail = PapertrailProvider()

        # HACK: other way methods are not called.
        for line in papertrail.get_logs(self.start_date, self.end_date):
            pass

        self.assertTrue(mock_s3_method.called)
        self.assertFalse(mock_cloud_method.called)

    @freeze_time(end_date)
    @patch.object(PapertrailProvider, 'get_from_cloud')
    @patch.object(PapertrailProvider, 'get_from_s3')
    def test_get_log_less_than_hour_ago(self, mock_s3_method, mock_cloud_method):
        papertrail = PapertrailProvider()

        # HACK: other way methods are not called.
        for line in papertrail.get_logs('2016-11-05 14:44:00', self.end_date):
            pass

        self.assertFalse(mock_s3_method.called)
        self.assertTrue(mock_cloud_method.called)