from django.contrib import admin

# Register your models here.


from .models import DateDimension, Hospital,  WaitingTime

admin.site.register(DateDimension)
admin.site.register(Hospital)
admin.site.register(WaitingTime)

