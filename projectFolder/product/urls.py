from django.conf.urls import url
from . import views

urlpatterns = [
	url(r'^$', views.product_list, name='product_list'),
    url(r'^(?P<product>[a-z]+)/$', views.viz, name='detail'),
    #url(r'^data/(?P<product>[a-z]+.csv)/$', views.viz, name='detail'),
]

