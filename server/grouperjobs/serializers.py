from rest_framework import serializers
from combinedjobs.models import CombinedJob


class GrouperJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = CombinedJob
        fields = ["id",
                  "created",
                  "report_title",
                  "grouping_method",
                  "main_keyword_grouping_accuracy",
                  "variant_keyword_grouping_accuracy",
                  ]
