from rest_framework.parsers import MultiPartParser
from fetcherjobs.models import FetcherJob
from fetcherjobs.serializers import FetcherJobSerializer
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, parsers
from utils.celery import run_fetcher
from utils.constants.cloudconstants import *

from utils.logutils import *
from utils.fetcherutils import write_current_output_file, write_output_file


class FetcherJobList(APIView):
    parser_classes = (parsers.JSONParser,)

    def put(self, request, format=None):
        """
        Add a fetcher job in celery task queue
        :param request: request data and headers
        :param format: request format
        :return: response
        """
        try:
            full_path = request.data["input_file_path"]
            job_id = request.data["job_id"]
            region = f"{request.data['location']}"
            competitor_domains = request.data['competitor_domains']
            ignore_special_characters = True
            if "ignore_special_characters" in request.data and request.data["ignore_special_characters"] == "false":
                ignore_special_characters = False
            task = run_fetcher.delay(full_path, serp_api_key, job_id, region, request.data['gl'],
                                     request.data["search_engine"], "fetcher",
                                     request.data["target_domain"], competitor_domains,
                                     ignore_special_characters)
        except Exception as inst:
            print(inst)
            return Response({"status": "failed"}, status=status.HTTP_406_NOT_ACCEPTABLE)
        else:
            return Response({"status": "running"}, status=status.HTTP_201_CREATED)

    def get(self, request, format=None):
        jobs = FetcherJob.objects.all()
        serializer = FetcherJobSerializer(jobs, many=True)
        return Response(serializer.data)

    def post(self, request, format=None):
        serializer = FetcherJobSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status.HTTP_201_CREATED)
        return Response(serializer.errors, status.HTTP_400_BAD_REQUEST)


class FetcherJobDetail(APIView):
    def get_object(self, pk):
        try:
            return FetcherJob.objects.get(pk=pk)
        except FetcherJob.DoesNotExist:
            raise Http404

    def get(self, request, job_id, format=None):
        """
        Add a handler to return the output file of the present snapshots at the time of request
        :param job_id: id for which data will be fetched to a current output file from snapshots 
        :param format: request format
        :return: response
        """

        try:
            out, bulk = write_current_output_file(job_id, "fetcher")
            if not out or not bulk:
                return Response({"message": "No snapshots found"}, status=status.HTTP_404_NOT_FOUND)
            return Response({"fetcher_out_path": out, "fetcher_bulk_out_path": bulk})
        except Exception as e:
            return Response({"status": f"{e}"}, status=status.HTTP_406_NOT_ACCEPTABLE)

    def put(self, request, job_id, format=None):
        """
        Add a resume handler that will resume fetcher job in celery task queue
        :param request: request data and headers
        :param job_id: id for which fetcher job will be resumed
        :param format: request format
        :return: response
        """
        try:

            # To get the single DataFrame having all the snapshot files data merged in it
            count, current_output_df = write_output_file(job_id, keep_snapshots=True, failure=True, job_type="fetcher")
            if not count:
                count = 0
                already_searched_keywords_list = []
                full_path = request.data["input_file_path"]
                region = f"{request.data['location']}"
                competitor_domains = request.data['competitor_domains']
                ignore_special_characters = True
                if "ignore_special_characters" in request.data and request.data["ignore_special_characters"] == "false":
                    ignore_special_characters = False
                # return Response({"status":"hello"})
                task = run_fetcher.delay(full_path, serp_api_key, job_id, region, request.data['gl'],
                                         request.data["search_engine"], "fetcher",
                                         request.data["target_domain"], competitor_domains,
                                         ignore_special_characters, count, already_searched_keywords_list)
            else:
                # Getting the keywords coloumn from the DataFrame
                output_keywords_series = current_output_df.iloc[:, 0]
                # bulk_output_keywords_series = current_bulk_output_df.iloc[:,0]

                # Converting the series above into lists
                output_keywords_list = output_keywords_series.tolist()
                # bulk_output_keywords_list = bulk_output_keywords_series.tolist()

                # Removing duplicate keywords from the lists
                already_searched_keywords_list = list(dict.fromkeys(output_keywords_list))
                # already_searched_bulk_keywords_list=list(dict.fromkeys(bulk_output_keywords_list))
                # This unique list will be passed to run_fetcher

                full_path = request.data["input_file_path"]
                region = f"{request.data['location']}"
                competitor_domains = request.data['competitor_domains']
                ignore_special_characters = True
                if "ignore_special_characters" in request.data and request.data["ignore_special_characters"] == "false":
                    ignore_special_characters = False
                task = run_fetcher.delay(full_path, serp_api_key, job_id, region, request.data['gl'],
                                         request.data["search_engine"], "fetcher",
                                         request.data["target_domain"], competitor_domains,
                                         ignore_special_characters, count, already_searched_keywords_list)
        except Exception as inst:
            print(inst)
            return Response({"status": inst}, status=status.HTTP_406_NOT_ACCEPTABLE)
        else:
            return Response({"status": "running"}, status=status.HTTP_201_CREATED)

    def get(self, request, format=None):
        jobs = FetcherJob.objects.all()
        serializer = FetcherJobSerializer(jobs, many=True)
        return Response(serializer.data)

    def delete(self, request, pk, format=None):
        job = self.get_object(pk)
        job.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
