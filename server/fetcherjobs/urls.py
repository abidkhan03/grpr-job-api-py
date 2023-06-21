from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from fetcherjobs import views

urlpatterns = [
    path('job/fetcher/', views.FetcherJobList.as_view()),
    path('job/fetcher/<str:job_id>/', views.FetcherJobDetail.as_view()),
    path('job/fetcher/resume/<str:job_id>/', views.FetcherJobDetail.as_view()),
]

urlpatterns = format_suffix_patterns(urlpatterns)
