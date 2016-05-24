from django.shortcuts import render
from django.views import generic
from .models import Item, Category

import json
import csv


def home(request):
	categories = Category.objects.order_by('category_name')
	return render(request, 'reviewapp/category_list.html', {'categories': categories})


def item_list(request, category):
	items = Item.objects.filter(category_id = category)
	items = items.order_by('ASIN')
	return render(request, 'reviewapp/item_list.html', {'items': items})

def viz(request, item_ASIN):
	#reader = csv.reader("data/cellphone.csv")
	
	# with open('reviewapp/data/cellphone.json') as data_file:    
	#     data = json.load(data_file)
	#     data = json.dumps(data)

	# data = data.replace("&quot;", "\"")
	# posList = data["pos"]
	item = Item.objects.filter(ASIN = item_ASIN)
	
	return render(request, 'reviewapp/visualization.html', {
            'item': item[0], 'item_ASIN': item_ASIN,
        })
