from django.db import models


class FetcherJob(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    report_title = models.TextField()
    search_engine = models.CharField(max_length=50, blank=True, default='')
    location = models.CharField(max_length=250, blank=True, default='')
    language = models.CharField(max_length=100, blank=True, default='')

    class Meta:
        ordering = ['created']
