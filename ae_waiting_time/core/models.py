from django.db import models

# Create your models here.

class DateDimension(models.Model):
    key = models.CharField(max_length=100, unique=True)
    date_time = models.DateTimeField(null=False, blank=False)

    def __str__(self):
        return self.key

class Hospital(models.Model):
    en_name = models.CharField(max_length=100)
    ch_name = models.CharField(max_length=100)
    short_ch_name = models.CharField(max_length=100)
    latitude = models.FloatField()
    longitude = models.FloatField()

    def __str__(self):
        return self.short_ch_name

class WaitingTime(models.Model):
    class Meta:
        unique_together = (('date', 'hospital'),)
    date = models.ForeignKey(DateDimension, on_delete=models.CASCADE)
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE)
    waiting_time_raw = models.CharField(max_length=100)
    waiting_time = models.DecimalField(max_digits=10, decimal_places=3)

    def __str__(self):
        return str(self.date) + ' ' + str(self.hospital)
