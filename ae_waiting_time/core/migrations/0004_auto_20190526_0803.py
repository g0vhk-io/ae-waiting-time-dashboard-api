# Generated by Django 2.0 on 2019-05-26 08:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0003_auto_20190526_0208'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datedimension',
            name='key',
            field=models.CharField(max_length=100, unique=True),
        ),
    ]
