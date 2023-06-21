import concurrent.futures
from threading import Lock
from utils.constants.celeryconstants import celery_broker_url,snapshots_count
from utils.fetcherutils import *
from utils.grouperutils import *
from utils.constants.fetcherconstants import *
from utils.snowflakeutils import save_to_snowflake
from utils.socketutils import *
from utils.constants.nodeconstants import *
from utils.constants.fileconstants import *
from utils.cloudutils import *
import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'server.settings')
app = Celery('jobs-handler', broker=celery_broker_url)
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()


@app.task
def run_fetcher(input_file: str, api_key: str, job_id: str, region: str = REGION, gl: str = GL,
                search_engine: str = SEARCH_ENGINE, job_type: str = "fetcher", target_domain: str = "",
                competitor_domains: List[str] = [], ignore_special_characters: bool = True,
                snap_shot_number:int = 0, already_searched_keywords_list: List[str] = []):
    """
    This function is used to run the fetcher job.
    :param input_file: The input file for the fetcher job.
    :param api_key: The api key for the serp api.
    :param job_id: The job id for the fetcher job.
    :param region: The region setting for serp api.
    :param search_engine: The region setting for serp api.
    :param job_type: fetcher or combined
    :param target_domain: The target domain for the fetcher job.
    :param competitor_domains: The competitor domains for the fetcher job.
    :param ignore_special_characters: Flag to determine if special characters are to be omitted.
    :param snap_shot_number: counter to avoid overwritiing of snapshots while resuming the job
    :param already_searched_keywords_list: list containing keywords that are already searched from the given input file
    :return:
    """
    out_file = ""
    bulk_file = ""
    print(len(competitor_domains))
    try:
        connect_to_socket(node_server_url)
        increment = 8
        if job_type == 'fetcher':
            send_signal(status_channel, {"jobId": job_id, "status": "running", "type": job_type, "path": ""})
            log = "downloading input file from gcs..."
            signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
        else:
            increment = increment // 2
        args = Arguments()
        if isinstance(input_file, str):
            args.input_file_path = input_file
        else:
            job_logger.error("invalid type input_file argument")
        if isinstance(api_key, str):
            args.api_key = api_key
        else:
            job_logger.error("invalid type api_key")
        if isinstance(region, str):
            args.region = region
        else:
            args.region = REGION
        if isinstance(search_engine, str):
            args.search_engine = search_engine
        else:
            args.search_engine = SEARCH_ENGINE
        if isinstance(gl, str):
            args.gl = gl
        else:
            args.gl = GL

        query_queue = get_input_queries(args,already_searched_keywords_list)

        progress = 5
        progress_logger.info({"jobId": job_id, "progress": progress})
        total_queries = len(query_queue)
        send_signal(input_size_channel, {"jobId": job_id, "type": job_type, "inputSize": total_queries})
        i = 0
        ignore_count = 0
        count = [snap_shot_number]
        snapshot_record_count = snapshots_count
        lock = Lock()
        key_link_dict = []
        bulk_key_link_dict = []
        if len(query_queue) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=100) as exe:
                for query in query_queue:
                    if not ignore_special_characters or (is_string_specialcharacter_less.match(query.keyword) is not None):
                        i = i + 1
                        futures=[exe.submit(get_keyword_links, query, args, key_link_dict, bulk_key_link_dict,lock, count, snapshot_record_count, job_id, job_type,
                                i, total_queries,
                                target_domain, competitor_domains, increment)]
                    else:
                        ignore_count = ignore_count + 1

            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

        
        if key_link_dict:
            write_snapshot_file(key_link_dict, bulk_key_link_dict,lock, count, snapshot_record_count, job_id, job_type=job_type)

        log = f"Ignored {ignore_count} keyword(s) with special characters"
        signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
        log = f"finished fetching data for {i} keyword(s)"
        signal_logger.info({"jobId": job_id, "type": "fetcher", "log": f"[{get_time_stamp()}] {log}"})

        snapshots_path,out_file, bulk_file = write_output_file(job_id,keep_snapshots=False,failure=False, job_type=job_type)
        
        job_logger.info("Fetcher completed!")
        upload_file_path = f"processed/fetcher/" + job_id + ".csv"
        bulk_upload_file_path = f"processed/fetcher/bulk/" + job_id + ".csv"

        if job_type == "fetcher":
            log = "uploading file to cloud..."
            signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
            upload_to_google_cloud(file_path=out_file, upload_file_path=upload_file_path)
            upload_to_google_cloud(file_path=bulk_file, upload_file_path=bulk_upload_file_path)
            send_signal(status_channel,
                        {"jobId": job_id, "status": "completed", "type": job_type, "path": upload_file_path,
                         "bulkPath": bulk_upload_file_path})
            progress = 100
            progress_logger.info({"jobId": job_id, "progress": progress})

            log = "Job finished successfully"
            signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})

            # Delete snapshots
            delete_snapshots(snapshots_path, job_id)
        return snapshots_path,out_file, bulk_file
    except KeyError as e:
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"Invalid key: {str(e)}.",
                     "path": ""})
    except ConnectionError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"{str(e)}.",
                     "path": ""})
    except AttributeError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"No such attribute{str(e)}.",
                     "path": ""})
    except Exception as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"{str(e)}.",
                     "path": ""})
    return out_file, bulk_file

@app.task
def run_grouper(input_file, threshold, job_id, hard_threshold=None, job_type: str = "grouper", calc_rank: bool = False,
                target_domain: str = "", organic_results_count: int = 10, competitor_domains: List[str] = [],
                no_of_clusters: int = 5):
    """
    This function is used to run the grouper job.
    :param input_file: input file path
    :param threshold: threshold value
    :param job_id: job id for the grouper job.
    :param hard_threshold: hard threshold value
    :param job_type: grouper or combined
    :param calc_rank: calculate rank or not
    :param target_domain: target domain
    :param organic_results_count: organic results count
    :param competitor_domains: competitor domains
    :param no_of_clusters: no of clusters
    :return: None
    """
    try:
        connect_to_socket(node_server_url)
        progress = 2
        if job_type == 'grouper':
            progress_logger.info({"jobId": job_id, "progress": progress})
            send_signal(status_channel, {"jobId": job_id, "status": "running", "type": job_type, "path": ""})
            log = "downloading input file from gcs..."
            signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
        orc = 10
        if hard_threshold is None:
            orc = organic_results_count

        key_link_dict = read_input(input_file_path=input_file, job_type=job_type, calc_rank=calc_rank,
                                   target_domain=target_domain,
                                   organic_results_count=orc,
                                   competitor_domains=competitor_domains)

        if job_type == "grouper":
            send_signal(input_size_channel, {"jobId": job_id, "type": job_type, "inputSize": len(key_link_dict)})
            log = "successfully downloaded input from gcs"
            signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
            progress = progress + 6
            progress_logger.info({"jobId": job_id, "progress": progress})
            
        key_link_dict = group_nodes(key_link_dict=key_link_dict, threshold=threshold,
                                    job_id=job_id, job_type=job_type, calc_sub_group=(hard_threshold is not None))

        if hard_threshold is not None:
            key_link_dict = calc_sub_groups(key_link_dict=key_link_dict, threshold=hard_threshold, job_id=job_id,
                                            job_type=job_type, organic_results_count=organic_results_count)

        now = datetime.now()
        time_stamp = now.strftime("%Y%m%d_%H_%M_%S")

        output_file_name = f"{project_base_dir}/processed/grouper/grouper_output_" + time_stamp + ".csv"
        # mkw_ctr_dict, highest_vol_mkw_arr = create_clusters(key_link_dict=key_link_dict, num_clusters=no_of_clusters,
        #                                                     job_id=job_id,
        #                                                     job_type=job_type)
        # assign_clusters(key_link_dict=key_link_dict, mkw_ctr_dict=mkw_ctr_dict)
        write_results(output_file_path=output_file_name, key_link_dict=key_link_dict,
                      contains_sub_groups=(hard_threshold is not None), job_id=job_id, job_type=job_type)

        progress = 90
        progress_logger.info({"jobId": job_id, "progress": progress})

        upload_file_path = "processed/grouper/" + job_id + ".csv"
        if job_type == "grouper":
            upload_to_google_cloud(file_path=output_file_name, upload_file_path=upload_file_path)
            send_signal(status_channel,
                        {"jobId": job_id, "status": "completed", "type": job_type, "path": upload_file_path})
            save_to_snowflake(job_id=job_id, file_path=output_file_name)
            log = "Job finished successfully"
            signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
            progress = 100
            progress_logger.info({"jobId": job_id, "progress": progress})
        return output_file_name
    except KeyError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"Invalid key: {str(e)}.",
                     "path": ""})
    except ConnectionError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"{str(e)}.",
                     "path": ""})
    except AttributeError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"No such attribute{str(e)}.",
                     "path": ""})
    except Exception as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": job_type,
                     "reasonOfFailure": f"{str(e)}.",
                     "path": ""})
        log = "Job failed"
        signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})


@app.task
def run_combined_job(input_file, threshold, api_key: str, job_id, region: str = REGION, gl: str = GL,
                     search_engine: str = SEARCH_ENGINE, hard_threshold=None,
                     target_domain: str = "", competitor_domains: List[str] = [],
                     ignore_special_characters: bool = True, organic_results_count: int = 10,
                     no_of_clusters: int = 5,snap_shot_number: int = 0,
                     already_searched_keywords_list: List[str] = []):
    """
    This function is used to run the combined job
    :param input_file: The input file path
    :param threshold: The threshold value
    :param api_key: The SERP API api key
    :param job_id: The job id
    :param region: The region setting for SERP API
    :param search_engine: The search engine setting for SERP API
    :param hard_threshold: The hard threshold value
    :param target_domain: The target domain
    :param competitor_domains: List of competitor domains
    :param ignore_special_characters: Flag to ignore special characters
    :param organic_results_count: The organic results count to consider for grouping
    :param no_of_clusters: The number of clusters to be created
    :param snap_shot_number: counter to avoid overwriting of snapshots while resuming the job
    :param already_searched_keywords_list: list containing keywords that are already searched from the given input file
    :return: None
    """
    grouper_out_file = ""
    try:
        connect_to_socket(node_server_url)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "running", "type": "combined", "fetcherPath": "", "grouperPath": ""})
        snapshot_files_path,fetcher_out_file, fetcher_bulk_out_file = run_fetcher(input_file=input_file, api_key=api_key, job_id=job_id,
                                                              region=region, gl=gl, search_engine = search_engine, 
                                                              job_type = "combined", target_domain = target_domain,
                                                              competitor_domains = competitor_domains,
                                                              ignore_special_characters = ignore_special_characters,
                                                              snap_shot_number = snap_shot_number, 
                                                              already_searched_keywords_list = already_searched_keywords_list)
        fetcher_upload_file_path = "processed/fetcher/" + job_id + ".csv"
        fetcher_bulk_upload_file_path = "processed/fetcher/bulk/" + job_id + ".csv"
        log = "Fetcher completed! Uploading processed file to cloud..."
        signal_logger.info({"jobId": job_id, "type": "combined", "log": f"[{get_time_stamp()}] {log}"})
        upload_to_google_cloud(file_path=fetcher_out_file, upload_file_path=fetcher_upload_file_path)
        upload_to_google_cloud(file_path=fetcher_bulk_out_file, upload_file_path=fetcher_bulk_upload_file_path)

        progress = 50
        progress_logger.info({"jobId": job_id, "progress": progress})
        send_signal(status_channel,
                    {"jobId": job_id, "status": "running", "type": "combined", "fetcherPath": fetcher_upload_file_path,
                     "fetcherBulkPath": fetcher_bulk_upload_file_path,
                     "grouperPath": ""})

        grouper_out_file = run_grouper(input_file=fetcher_out_file, threshold=threshold,
                                       hard_threshold=hard_threshold, job_id=job_id, job_type="combined",
                                       organic_results_count=organic_results_count, no_of_clusters=no_of_clusters)
        final_upload_file_path = "processed/combined/" + job_id + ".csv"
        log = "Grouper completed! Uploading processed file to cloud..."
        signal_logger.info({"jobId": job_id, "type": "combined", "log": f"[{get_time_stamp()}] {log}"})
        upload_to_google_cloud(file_path=grouper_out_file, upload_file_path=final_upload_file_path)
        progress = 100
        progress_logger.info({"jobId": job_id, "progress": progress})
        send_signal(status_channel,
                    {"jobId": job_id, "status": "completed", "type": "combined",
                     "fetcherPath": fetcher_upload_file_path,
                     "fetcherBulkPath": fetcher_bulk_upload_file_path,
                     "grouperPath": final_upload_file_path})
        save_to_snowflake(job_id=job_id, file_path=grouper_out_file)

        delete_snapshots(snapshot_files_path, job_id)
        
        log = "Job finished successfully"
        signal_logger.info({"jobId": job_id, "type": "combined", "log": f"[{get_time_stamp()}] {log}"})
        
        return grouper_out_file
    except KeyError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": "combined",
                     "reasonOfFailure": f"Invalid key: {str(e)}.",
                     "fetcherPath": "", "fetcherBulkPath": "", "grouperPath": ""})
    except ConnectionError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": "combined",
                     "reasonOfFailure": f"{str(e)}.",
                     "fetcherPath": "", "fetcherBulkPath": "", "grouperPath": ""})
    except AttributeError as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": "combined",
                     "reasonOfFailure": f"No such attribute{str(e)}.",
                     "fetcherPath": "", "fetcherBulkPath": "", "grouperPath": ""})
    except Exception as e:
        connect_to_socket(node_server_url)
        job_logger.error(e)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "failed", "type": "combined",
                     "reasonOfFailure": f"{str(e)}.",
                     "fetcherPath": "", "fetcherBulkPath": "", "grouperPath": ""})
    return grouper_out_file
