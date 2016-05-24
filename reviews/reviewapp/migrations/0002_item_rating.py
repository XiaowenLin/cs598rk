# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reviewapp', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='item',
            name='rating',
            field=models.DecimalField(default=3.0, max_digits=3, decimal_places=2),
        ),
    ]
