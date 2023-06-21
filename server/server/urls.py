from django.urls import path, include

urlpatterns = [
    path('japi/', include('combinedjobs.urls')),
    path('japi/', include('grouperjobs.urls')),
    path('japi/', include('fetcherjobs.urls')),
]
