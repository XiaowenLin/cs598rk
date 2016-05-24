from django.db import models
from django.utils import timezone

DEFAULT_CATEGORY = 1

class Category(models.Model):
    author = models.ForeignKey('auth.User')
    DEFAULT_PK = 1
    created_date = models.DateTimeField(
            default=timezone.now)
    published_date = models.DateTimeField(
            blank=True, null=True)
    category_name = models.CharField(max_length=200)

class Item(models.Model):
    product_name = models.CharField(max_length=200)
    ASIN = models.CharField(max_length=20)
    seller = models.CharField(max_length=40)
    category = models.ForeignKey(Category, default = Category.DEFAULT_PK)
    rating = models.DecimalField(default= 3.0, max_digits=3, decimal_places=2)

    def publish(self):
        self.published_date = timezone.now()
        self.save()

    def __str__(self):
        return self.product_name

