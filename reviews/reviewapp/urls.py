from django.conf.urls import url
from . import views

urlpatterns = [
	url(r'^$', views.home, name='home'),
	url(r'^^(?P<category>[0-9]+)/$$', views.item_list, name='item_list'),
    url(r'^([0-9]+)/(?P<item_ASIN>[A-Z0-9]+)$', views.viz, name='detail'),
    #url(r'^data/(?P<product>[a-z]+.csv)/$', views.viz, name='detail'),
]

