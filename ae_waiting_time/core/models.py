from django.db import models

# Create your models here.

class DateDimension(models.Model):
    key = models.CharField(max_length=100, primary_key=True)
    date_time = models.DateTimeField(null=False, blank=False)
    pass

class Hospital(models.Model):
    en_name = models.CharField(max_length=100, primary_key=True)
    ch_name = models.CharField(max_length=100)
    short_ch_name = models.CharField(max_length=100)
    latitude = models.FloatField()
    longitude = models.FloatField()

class WaitingTime(models.Model):
    class Meta:
        unique_together = (('date', 'hospital'),)
    date = models.ForeignKey(DateDimension, on_delete=models.CASCADE)
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE)
    waiting_time_raw = models.CharField(max_length=100)
    waiting_time = models.IntegerField()
