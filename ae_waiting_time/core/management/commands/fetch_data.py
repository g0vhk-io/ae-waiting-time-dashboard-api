# -*- coding: utf-8 -*-
"""

@author: eric-2048 howawong

"""

from django.core.management.base import BaseCommand, CommandError
from datetime import date, datetime, timedelta
import requests
from multiprocessing.pool import ThreadPool
from core.models import *
from django.db.utils import IntegrityError

class Command(BaseCommand):
    help = 'Fetch data from data.gov.hk'
    FORMAT = '%Y%m%d'

    def add_arguments(self, parser):
        parser.add_argument('date', type=str, nargs='?')

    def retrieve_file_list(self, start_date, end_date, url = "http://www.ha.org.hk/opendata/aed/aedwtdata-en.json"):
        current_date = start_date
        timestamps = []
        while current_date <= end_date:
            s = datetime.strftime(current_date, self.FORMAT)
            for i in range(0, 24):
                for j in [0, 15, 30, 45]:
                    timestamps.append('%s-%.2d%.2d' % (s, i, j))
            current_date = current_date + timedelta(days=1)
        print(timestamps)
        return ['https://api.data.gov.hk/v1/historical-archive/get-file?url=http%3A%2F%2Fwww.ha.org.hk%2Fopendata%2Faed%2Faedwtdata-en.json&time=' + timestamp for timestamp in timestamps]

    def raw_time_to_int(self, s):
        val_map  = {"Around 1 hour" : 0.5, "Over 1 hour" : 1.5, "Over 2 hours" : 2.5,
            "Over 3 hours" : 3.5, "Over 4 hours":4.5, "Over 5 hours" : 5.5,
             "Over 6 hours" : 6.5, "Over 7 hours" : 7.5, "Over 8 hours" : 8.5}
        return val_map[s]

    def handle(self, *args, **options):
        today = date.today()
        if options['date']:
            today = datetime.strptime(options['date'], self.FORMAT).date()
        start_date = today + timedelta(days=-7)
        urls = self.retrieve_file_list(start_date, today)
        responses = ThreadPool(8).map(requests.get, urls)
        responses = [r.json() for r in responses]
        hospitals = [h for h in Hospital.objects.all()]
        records = []
        for r in responses:
            date_key = r['updateTime'] 
            update_time = datetime.strptime(date_key, '%d/%m/%Y %I:%M%p')
            date_dim, created = DateDimension.objects.get_or_create(
                    key=date_key,date_time=update_time
                    )
            for wh in r['waitTime']:
                h = [h for h in hospitals if h.en_name == wh['hospName']][0]
                top_wait = wh['topWait']
                wh_record = WaitingTime(date=date_dim, hospital=h, waiting_time_raw=top_wait, waiting_time=self.raw_time_to_int(top_wait))
                records.append(wh_record)
            try:
                WaitingTime.objects.bulk_create(records)
            except IntegrityError:
                print('Already inserted')
            records = []
            print(date_key + ' done')
