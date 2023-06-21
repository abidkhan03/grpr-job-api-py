from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from combinedjobs import views

urlpatterns = [
    path('job/combined/', views.CombinedJobList.as_view()),
    path('job/combined/<int:pk>/', views.CombinedJobDetail.as_view()),
    path('job/combined/resume/<str:job_id>/', views.CombinedJobDetail.as_view()),
]

urlpatterns = format_suffix_patterns(urlpatterns)
