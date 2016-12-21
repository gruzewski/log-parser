from __future__ import absolute_import

from abc import ABCMeta, abstractmethod

from six import add_metaclass


@add_metaclass(ABCMeta)
class LogProvider():
    @abstractmethod
    def get_logs(self, start_date, end_date, search_patter='', interval=15):
        pass