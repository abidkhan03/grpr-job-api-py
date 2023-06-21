from rest_framework.parsers import JSONParser
from combinedjobs.models import CombinedJob
from combinedjobs.serializers import CombinedJobSerializer
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from utils.celery import run_combined_job
from utils.logutils import *
from utils.constants.fileconstants import *
from utils.fetcherutils import write_output_file


class CombinedJobList(APIView):
    parser_classes = (JSONParser,)

    def put(self, request, format=None):
        """
        Add a combined job in celery task queue
        :param request: request data and headers
        :param format: request format
        :return: response
        """
        try:
            full_path = request.data["input_file_path"]
            hard_threshold = None
            region = f'{request.data["location"]}'
            search_engine = f'{request.data["search_engine"]}'
            competitor_domains = request.data['competitor_domains']
            organic_results_count = 10
            no_of_clusters = 5
            if request.data["grouping_method"] == "Main + Variants":
                hard_threshold = int(request.data["variant_keyword_grouping_accuracy"])
            if "no_of_clusters" in request.data:
                no_of_clusters = int(request.data["no_of_clusters"])
            ignore_special_characters = True
            if "ignore_special_characters" in request.data and request.data["ignore_special_characters"] == "false":
                ignore_special_characters = False
            if "organic_results_count" in request.data:
                organic_results_count = int(request.data["organic_results_count"])
                job_logger.info(f"Organic Result Count: {organic_results_count}")
            task = run_combined_job.delay(full_path,
                                          int(request.data["main_keyword_grouping_accuracy"]),
                                          config('API_KEY'),
                                          request.data["job_id"],
                                          region,
                                          request.data["gl"],
                                          search_engine,
                                          hard_threshold,
                                          request.data["target_domain"],
                                          competitor_domains,
                                          ignore_special_characters,
                                          organic_results_count,
                                          no_of_clusters)
        except Exception as inst:
            job_logger.error(inst)
            return Response({"status": "failed"}, status=status.HTTP_406_NOT_ACCEPTABLE)
        else:
            return Response({"status": "running"}, status=status.HTTP_201_CREATED)

    def get(self, request, format=None):
        jobs = CombinedJob.objects.all()
        serializer = CombinedJobSerializer(jobs, many=True)
        return Response(serializer.data)

    def post(self, request, format=None):
        serializer = CombinedJobSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status.HTTP_201_CREATED)
        return Response(serializer.errors, status.HTTP_400_BAD_REQUEST)


class CombinedJobDetail(APIView):
    def get_object(self, pk):
        try:
            return CombinedJob.objects.get(pk=pk)
        except CombinedJob.DoesNotExist:
            raise Http404

    def get(self, request, pk, format=None):
        job = self.get_object(pk)
        serializer = CombinedJobSerializer(job)
        return Response(serializer.data)

    def put(self, request, job_id, format=None):
        """
        Add a resume handler that will resume combined job in celery task queue
        :param request: request data and headers
        :param job_id: id for which fetcher job will be resumed
        :param format: request format
        :return: response
        """
        try:
            # To get the single DataFrame having all the snapshot files data merged in it
            count, current_output_df = write_output_file(job_id, keep_snapshots=True, failure=True, job_type="combined")
            if not count:
                count = 0
                already_searched_keywords_list = []
                # Input file on what job needs to be resumed
                full_path = request.data["input_file_path"]

                hard_threshold = None
                region = f'{request.data["location"]}'

                search_engine = f'{request.data["search_engine"]}'
                competitor_domains = request.data['competitor_domains']

                organic_results_count = 10
                no_of_clusters = 5
                if request.data["grouping_method"] == "Main + Variants":
                    hard_threshold = int(request.data["variant_keyword_grouping_accuracy"])

                if "no_of_clusters" in request.data:
                    no_of_clusters = int(request.data["no_of_clusters"])

                ignore_special_characters = True
                if "ignore_special_characters" in request.data and request.data["ignore_special_characters"] == "false":
                    ignore_special_characters = False

                if "organic_results_count" in request.data:
                    organic_results_count = int(request.data["organic_results_count"])
                    print(f"Organic Result Count: {organic_results_count}")
                task = run_combined_job.delay(full_path,
                                              int(request.data["main_keyword_grouping_accuracy"]),
                                              config('API_KEY'),
                                              job_id,
                                              region,
                                              request.data["gl"],
                                              search_engine,
                                              hard_threshold,
                                              request.data["target_domain"],
                                              competitor_domains,
                                              ignore_special_characters,
                                              organic_results_count,
                                              no_of_clusters,
                                              count,
                                              already_searched_keywords_list
                                              )
            else:
                # Getting keywords coloumn from the DataFrame
                output_keywords_series = current_output_df.iloc[:, 0]
                # bulk_output_keywords_series = current_bulk_output_df.iloc[:,0]

                # Converting the output from series type to list
                output_keywords_list = output_keywords_series.tolist()
                # bulk_output_keywords_list = bulk_output_keywords_series.tolist()

                # Removing duplicate keywords from the lists
                already_searched_keywords_list = list(dict.fromkeys(output_keywords_list))
                # already_searched_bulk_keywords_list=list(dict.fromkeys(bulk_output_keywords_list))

                # Input file on what job needs to be resumed
                full_path = request.data["input_file_path"]

                hard_threshold = None
                region = f'{request.data["location"]}'

                search_engine = f'{request.data["search_engine"]}'
                competitor_domains = request.data['competitor_domains']

                organic_results_count = 10
                no_of_clusters = 5
                if request.data["grouping_method"] == "Main + Variants":
                    hard_threshold = int(request.data["variant_keyword_grouping_accuracy"])

                if "no_of_clusters" in request.data:
                    no_of_clusters = int(request.data["no_of_clusters"])

                ignore_special_characters = True
                if "ignore_special_characters" in request.data and request.data["ignore_special_characters"] == "false":
                    ignore_special_characters = False

                if "organic_results_count" in request.data:
                    organic_results_count = int(request.data["organic_results_count"])
                    print(f"Organic Result Count: {organic_results_count}")
                task = run_combined_job.delay(full_path,
                                              int(request.data["main_keyword_grouping_accuracy"]),
                                              config('API_KEY'),
                                              job_id,
                                              region,
                                              request.data["gl"],
                                              search_engine,
                                              hard_threshold,
                                              request.data["target_domain"],
                                              competitor_domains,
                                              ignore_special_characters,
                                              organic_results_count,
                                              no_of_clusters,
                                              count,
                                              already_searched_keywords_list
                                              )
        except Exception as inst:
            print(inst)
            return Response({"status": inst}, status=status.HTTP_406_NOT_ACCEPTABLE)
        else:
            return Response({"status": "running"}, status=status.HTTP_201_CREATED)

    def delete(self, request, pk, format=None):
        job = self.get_object(pk)
        job.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
