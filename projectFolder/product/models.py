from django.db import models
from django.utils import timezone


class Item(models.Model):
    author = models.ForeignKey('auth.User')
    product_name = models.CharField(max_length=200)
    ASIN = models.CharField(max_length=20)
    created_date = models.DateTimeField(
            default=timezone.now)
    published_date = models.DateTimeField(
            blank=True, null=True)

    def publish(self):
        self.published_date = timezone.now()
        self.save()

    def __str__(self):
        return self.product_name