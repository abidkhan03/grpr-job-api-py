from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from grouperjobs import views

urlpatterns = [
    path('job/grouper/', views.GrouperJobList.as_view()),
    path('job/grouper/<int:pk>/', views.GrouperJobDetail.as_view()),
]

urlpatterns = format_suffix_patterns(urlpatterns)
