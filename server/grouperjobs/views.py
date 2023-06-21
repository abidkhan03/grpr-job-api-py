from rest_framework.parsers import JSONParser
from grouperjobs.models import GrouperJob
from grouperjobs.serializers import GrouperJobSerializer
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from utils.celery import run_grouper


class GrouperJobList(APIView):
    parser_classes = (JSONParser,)

    def put(self, request, format=None):
        """
        Add a grouper job in celery task queue
        :param request: request data and headers
        :param format: request format
        :return: response
        """
        full_path = request.data["input_file_path"]
        hard_threshold = None
        if request.data["grouping_method"] == "Main + Variants":
            hard_threshold = int(request.data["variant_keyword_grouping_accuracy"])
        target_domain = ""
        organic_results_count = 10
        competitor_domains = []
        no_of_clusters = 5
        if "target_domain" in request.data:
            target_domain = request.data["target_domain"]
        if "organic_results_count" in request.data:
            organic_results_count = int(request.data["organic_results_count"])
        if "competitor_domains" in request.data:
            competitor_domains = request.data["competitor_domains"]
        if "no_of_clusters" in request.data:
            no_of_clusters = int(request.data["no_of_clusters"])

        task = run_grouper.delay(input_file=full_path,
                                 threshold=int(request.data["main_keyword_grouping_accuracy"]),
                                 job_id=request.data["job_id"],
                                 hard_threshold=hard_threshold,
                                 calc_rank=("target_domain" in request.data),
                                 target_domain=target_domain,
                                 organic_results_count=organic_results_count,
                                 competitor_domains=competitor_domains,
                                 no_of_clusters=no_of_clusters)
        return Response({"status": "running"}, status=status.HTTP_201_CREATED)

    def get(self, request, format=None):
        jobs = GrouperJob.objects.all()
        serializer = GrouperJobSerializer(jobs, many=True)
        return Response(serializer.data)

    def post(self, request, format=None):
        serializer = GrouperJobSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status.HTTP_201_CREATED)
        return Response(serializer.errors, status.HTTP_400_BAD_REQUEST)


class GrouperJobDetail(APIView):
    def get_object(self, pk):
        try:
            return GrouperJob.objects.get(pk=pk)
        except GrouperJob.DoesNotExist:
            raise Http404

    def get(self, request, pk, format=None):
        job = self.get_object(pk)
        serializer = GrouperJobSerializer(job)
        return Response(serializer.data)

    def put(self, request, pk, format=None):
        job = self.get_object(pk)
        serializer = GrouperJobSerializer(job, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk, format=None):
        job = self.get_object(pk)
        job.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
