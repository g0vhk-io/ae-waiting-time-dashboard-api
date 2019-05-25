from django.core.management.base import BaseCommand, CommandError
from datetime import date

class Command(BaseCommand):
    help = 'Fetch data from data.gov.hk'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        print('Hello World')
