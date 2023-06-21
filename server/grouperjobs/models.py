from django.db import models


class GrouperJob(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    report_title = models.TextField()
    grouping_method = models.CharField(max_length=50, blank=True, default='')
    main_keyword_grouping_accuracy = models.IntegerField()
    variant_keyword_grouping_accuracy = models.IntegerField()

    class Meta:
        ordering = ['created']
