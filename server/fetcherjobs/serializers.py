from rest_framework import serializers
from fetcherjobs.models import FetcherJob


class FetcherJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = FetcherJob
        fields = ["id",
                  "created",
                  "report_title",
                  "search_engine",
                  "location",
                  "language",
                  ]
