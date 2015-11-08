from django.shortcuts import render
from django.views import generic
from .models import Item

import json
import csv

def product_list(request):
	products = Item.objects.order_by('ASIN')
	return render(request, 'product/product_list.html', {'products': products})



def viz(request, product):
	#reader = csv.reader("data/cellphone.csv")
	'''
	with open('product/data/cellphone.json') as data_file:    
	    data = json.load(data_file)
	    data = json.dumps(data)

	data = data.replace("&quot;", "\"")
	'''
	return render(request, 'product/visualization.html', {
            'product': product,
            #'data': data,
        })
