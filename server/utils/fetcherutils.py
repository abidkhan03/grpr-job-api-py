import re, os
from typing import List
from serpapi import GoogleSearch
import csv
from statistics import median

from utils.commonutils import calculate_rank, get_fib_helper, remove_permalink, get_ctr_values, calc_competitor_score
from utils.constants.fetcherconstants import *
from utils.models.combinedmodels import Rank
from utils.models.fetchermodels import Node, Feature, Arguments, Link
from utils.logutils import *
from utils.constants.fileconstants import *
from utils.cloudutils import *

is_string_specialcharacter_less = re.compile("^[A-Za-z0-9 ]+$")

cpc_median = 1.00
cps_median = 0.35


def write_current_output_file(job_id: str, job_type: str = "fetcher"):
    """
        Merges the current snapshot files to make an output file
        :param job_id: id for which the task is to be done
        :param job_type: type of the current job
        :return: response
        """
    current_out_file = ""
    current_bulk_file = ""

    snapshots_path, current_out_file, current_bulk_file = write_output_file(job_id, keep_snapshots=True, failure=False,
                                                                            job_type=job_type)
    if not current_bulk_file or not current_out_file:
        return current_out_file, current_bulk_file
    now = datetime.now()
    time_stamp = now.strftime("%Y%m%d_%H_%M_%S")
    upload_file_path = f"processed/fetcher/current_output_" + time_stamp + job_id + ".csv"
    bulk_upload_file_path = f"processed/fetcher/bulk/current_output" + time_stamp + job_id + ".csv"

    if job_type == "fetcher":
        log = "uploading current output file to cloud..."
        signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
        upload_to_google_cloud(file_path=current_out_file, upload_file_path=upload_file_path)
        upload_to_google_cloud(file_path=current_bulk_file, upload_file_path=bulk_upload_file_path)
        send_signal(status_channel,
                    {"jobId": job_id, "status": "completed", "type": job_type, "path": upload_file_path,
                     "bulkPath": bulk_upload_file_path})
        progress = 100
        progress_logger.info({"jobId": job_id, "progress": progress})
        log = "made output files for current snapshots successfully"
        signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})

    return upload_file_path, bulk_upload_file_path


def get_keyword_links(query: Node, arguments: Arguments, key_link_dict: List[Node],
                      bulk_key_link_dict: List[Node], lock, count, snapshot_record_count, job_id: str,
                      job_type: str, number: int, total_queries: int, target_domain: str = "",
                      competitor_domains: List[str] = None, increment: int = 8):
    """
    Hits SERP API and gets the links for the keyword
    :param query: keyword to get links
    :param arguments: search arguments
    :param key_link_dict: list to store fetcher data
    :param bulk_key_link_dict: list to store fetcher data in bulk
    :param lock: lock passed with the shared data
    :param count: The snapshot number
    :param snapshot_record_count: threshold size for taking a snapshot
    :param job_id: id of the current job
    :param job_type: type of the current job
    :param number: the KW number being processed
    :param total_queries: total number of queries to be processed
    :param increment: increment to be added in progress per step
    :param target_domain: target domain to be ranked
    :param competitor_domains: list of competitor domains
    """
    params = {
        "engine": arguments.search_engine,
        "q": query.keyword,
        "location": arguments.region,
        "gl": arguments.gl,
        "async": False,
        "api_key": arguments.api_key,
        "num": 100
    }
    links = []
    bulk_links = []
    search = GoogleSearch(params)
    search_results = search.get_dict()
    misspelled = False
    connect_to_socket(node_server_url)
    features = Feature()
    if "search_information" in search_results:
        search_information = search_results["search_information"]
        if "spelling_fix" in search_information:
            misspelled = True

    if not misspelled:
        answer_box_link = ""
        if "answer_box" in search_results or "answer_box_list" in search_results:
            features.featured_snippet = True
        if "answer_box" in search_results:
            answer_box = search_results["answer_box"]
            features.answer_box = True
            if "type" in answer_box and answer_box["type"] == "organic_result":
                cur_link = Link()
                if "link" in answer_box:
                    answer_box_link = answer_box["link"]
                    cur_link.url = remove_permalink(answer_box["link"])
                if "title" in answer_box:
                    cur_link.title = answer_box["title"]
                if "snippet" in answer_box:
                    snippet: str = answer_box["snippet"]
                    cur_link.snippet = snippet.replace(";", "\";\"")
                else:
                    cur_link.snippet = ""
                cur_link.position = 1
                links.append(cur_link)
        key_rank = Rank()
        competitor_ranks = []
        if "organic_results" in search_results:
            organic_results = search_results["organic_results"]
            competitor_ranks = calculate_rank(organic_results=organic_results,
                                              answer_box_link=answer_box_link,
                                              target_domain=target_domain,
                                              competitor_domains=competitor_domains)
            key_rank = competitor_ranks[0]
            competitor_ranks = competitor_ranks[1:]
            bulk_organic_results = organic_results[:]
            organic_results = organic_results[:10]
            features.organic_result_count = len(organic_results)

            for result in bulk_organic_results:
                cur_link = Link()
                if "link" in result:
                    cur_link.url = remove_permalink(result["link"])
                if "position" in result:
                    cur_link.position = result["position"]
                if "title" in result:
                    cur_link.title = result["title"]
                if "snippet" in result:
                    snippet: str = result["snippet"]
                    cur_link.snippet = snippet.replace(";", "\";\"")
                else:
                    cur_link.snippet = ""
                bulk_links.append(cur_link)

            for result in organic_results:
                if "sitelinks_search_box" in result and result["sitelinks_search_box"]:
                    features.sitelinks_search_box = True

                if "sitelinks" in result:
                    if "expanded" in result["sitelinks"]:
                        features.sitelinks_expanded = True
                    if "inline" in result["sitelinks"]:
                        features.sitelinks_inline = True

                cur_link = Link()
                if "related_results" in result:
                    cur_link.related_results_count = len(result["related_results"])
                if "link" in result:
                    cur_link.url = remove_permalink(result["link"])
                if "position" in result:
                    cur_link.position = result["position"]
                if "title" in result:
                    cur_link.title = result["title"]
                if "snippet" in result:
                    snippet: str = result["snippet"]
                    cur_link.snippet = snippet.replace(";", "\";\"")
                else:
                    cur_link.snippet = ""
                links.append(cur_link)

        if "ads" in search_results:
            for ad in search_results["ads"]:
                if "block_position" in ad:
                    if ad["block_position"] == "top":
                        features.ad_result_top_count = features.ad_result_top_count + 1
                    if ad["block_position"] == "bottom":
                        features.ad_result_bottom_count = features.ad_result_bottom_count + 1
                    if ad["block_position"] == "right":
                        features.ad_result_right_count = features.ad_result_right_count + 1
        if "events_results" in search_results:
            features.events_results = True

        if "inline_images" in search_results:
            features.inline_images = True

        # inline_people_also_search_for
        if "inline_people_also_search_for" in search_results:
            features.inline_people_also_search_for = True

        # shopping_results
        if "shopping_results" in search_results:
            features.shopping_results = True

        # inline_videos
        if "inline_videos" in search_results:
            features.inline_videos = True

        # inline_video_carousels
        if "inline_video_carousels" in search_results:
            features.inline_video_carousels = True

        # knowledge_graph
        if "knowledge_graph" in search_results:
            features.knowledge_graph = True

        # local_results
        if "local_results" in search_results or "local_ads" in search_results:
            features.local_results = True

        # news_results
        if "news_results" in search_results:
            features.news_results = True

        # top_stories
        if "top_stories" in search_results:
            features.top_stories = True

        # inline_products
        if "inline_products" in search_results:
            features.inline_products = True

        # recipes_results
        if "recipes_results" in search_results:
            features.recipes_results = True

        # related_questions
        if "related_questions" in search_results:
            features.related_questions = True

        # twitter_results
        if "twitter_results" in search_results:
            features.twitter_results = True

        primary_intents = []
        secondary_intents = []
        if features.organic_result_count >= 5 and not (
                features.inline_products or features.ad_result_top_count > 0 or features.ad_result_bottom_count > 0 or
                features.ad_result_right_count > 0 or features.shopping_results):
            primary_intents.append(PrimarySearchIntent.INFORMATIONAL.value)
        if features.organic_result_count >= 5 and (
                features.inline_products or features.ad_result_top_count > 0 or features.ad_result_bottom_count > 0 or
                features.ad_result_right_count > 0 or features.shopping_results):
            primary_intents.append(PrimarySearchIntent.INVESTIGATIONAL.value)
        if features.sitelinks_expanded or features.sitelinks_search_box:
            primary_intents.append(PrimarySearchIntent.NAVIGATIONAL.value)
        if features.inline_videos or features.inline_images:
            secondary_intents.append(SecondarySearchIntent.VISUAL.value)
        if features.local_results:
            secondary_intents.append(SecondarySearchIntent.LOCAL.value)
        if features.top_stories:
            secondary_intents.append(SecondarySearchIntent.NEWS.value)

        if query.cps == -1.0:
            query.cps = cps_median
        if query.cpc == -1.0:
            query.cpc = cpc_median

        ctr_value, potential_ctr_value = get_ctr_values(serp_features=features,
                                                        ranking_position=key_rank.client_ranking_position)
        potential_traffic = potential_ctr_value * query.volume * query.cps
        current_traffic = ctr_value * query.volume * query.cps

        current_value = current_traffic * query.cpc
        potential_value = potential_traffic * query.cpc

        for comp_rank in competitor_ranks:
            cv, pv = get_ctr_values(serp_features=features,
                                    ranking_position=comp_rank.client_ranking_position)
            comp_rank.current_traffic = cv * query.volume * query.cps
            comp_rank.current_value = comp_rank.current_traffic * query.cpc

        bulk_key_link = Node()
        key_link = Node()

        key_link.keyword = query.keyword
        bulk_key_link.keyword = query.keyword
        key_link.volume = query.volume
        bulk_key_link.volume = query.volume
        key_link.links = links
        bulk_key_link.links = bulk_links
        key_link.primary_search_intents = primary_intents
        bulk_key_link.primary_search_intents = primary_intents
        key_link.secondary_search_intents = secondary_intents
        bulk_key_link.secondary_search_intents = secondary_intents
        key_link.rank = key_rank
        key_link.competitor_ranks = competitor_ranks[0:]
        bulk_key_link.rank = key_rank
        bulk_key_link.competitor_ranks = competitor_ranks[0:]

        key_link.difficulty = query.difficulty
        key_link.current_traffic = current_traffic
        key_link.current_value = current_value
        key_link.potential_value = potential_value
        key_link.potential_traffic = potential_traffic
        key_link.fibonacci_helper = get_fib_helper(rank_position=key_rank.client_ranking_position)
        key_link.cpc = query.cpc
        key_link.cps = query.cps

        bulk_key_link.difficulty = query.difficulty
        bulk_key_link.current_traffic = current_traffic
        bulk_key_link.current_value = current_value
        bulk_key_link.potential_value = potential_value
        bulk_key_link.potential_traffic = potential_traffic
        bulk_key_link.fibonacci_helper = get_fib_helper(rank_position=key_rank.client_ranking_position)
        bulk_key_link.cpc = query.cpc
        bulk_key_link.cps = query.cps

        key_link_dict.append(key_link)
        bulk_key_link_dict.append(bulk_key_link)

        with lock:
            if len(key_link_dict) >= snapshot_record_count:
                write_snapshot_file(key_link_dict, bulk_key_link_dict, lock, count, snapshot_record_count, job_id,
                                    job_type=job_type)

    total_processed = len(key_link_dict)
    step = (total_queries // 10)
    if total_processed % step == 0:
        progress = 5 + (increment * (total_processed / step))
        progress_logger.info({"jobId": job_id, "progress": progress})


def get_out_row(key_link: Node, link: Link):
    """
    Generates and returns a row for the output file
    :param key_link: Node object containing the keyword and it's respective fields
    :param link: Link object containing the link and it's respective fields
    :return: A list containing the row data
    """
    primary_intents = "/".join(key_link.primary_search_intents)
    secondary_intents = "/".join(key_link.secondary_search_intents)
    comp_score, comp_count = calc_competitor_score(key_link=key_link)
    out_row = [
        key_link.keyword, key_link.volume, link.url, link.position, link.title, link.snippet,
        primary_intents, secondary_intents, key_link.rank.client_ranking_url,
        key_link.rank.client_ranking_position, key_link.rank.client_url_ranking_count,
        key_link.cpc,
        key_link.cps,
        key_link.difficulty,
        key_link.current_traffic,
        key_link.potential_traffic,
        key_link.current_value,
        key_link.potential_value,
        key_link.fibonacci_helper,
        key_link.potential_value - key_link.current_value,
        key_link.volume - key_link.current_traffic,
        comp_score,
        comp_count,
        link.related_results_count
    ]
    for comp_rank in key_link.competitor_ranks:
        out_row.append(comp_rank.client_ranking_url)
        out_row.append(comp_rank.client_ranking_position)
        out_row.append(comp_rank.current_traffic)
        out_row.append(comp_rank.current_value)
    return out_row


def get_header_array(competitor_count: int = 0):
    """
    Generates and returns the header row for the output file
    :param competitor_count: The number of competitors to be included in the header row
    :return: A list containing the header row data
    """
    header_array = [KEYWORD, VOLUME, LINK, POSITION, TITLE, SNIPPET, PRIMARY_INTENTS, SECONDARY_INTENTS,
                    CLIENT_RANKING_URL, CLIENT_RANKING_POSITION, CLIENT_URL_RANKING_COUNT, "CPC", "CPS", "Difficulty",
                    "Current Traffic", "Potential Traffic", "Current Value", "Potential Value", "Fibonacci Helper",
                    "Value Opportunity", "Volume Opportunity", 'Competitor Score',
                    'Competitor ranking count', "Related Results Count"]
    competitor_number = 'A'
    for i in range(competitor_count):
        header_array.append(f'Competitor {competitor_number} ranking URL')
        header_array.append(f'Competitor {competitor_number} rank')
        header_array.append(f'Competitor {competitor_number} current traffic')
        header_array.append(f'Competitor {competitor_number} current value')
        competitor_number = chr(ord(competitor_number) + 1)

    return header_array


def write_snapshot_file(key_link_dict: List[Node], bulk_key_link_dict: List[Node], lock, count, snapshot_record_count,
                        job_id: str,
                        job_type: str = "fetcher"):
    """
    Writes the snapshot file of (snapshot_record_count(i.e 100)) fetched records to the snapshot directory
    :param key_link_dict: A list of Node objects containing the keyword and data for it's top results
    :param bulk_key_link_dict: A list of Node objects containing the keyword and data for all it's results
    :lock: lock is passed with shared data
    :param job_id: The job id
    :param job_type: The job type
    :return: None
    """

    connect_to_socket(node_server_url)
    count[0] += 1
    snapshot_file_name = f"{project_base_dir}/processed/fetcher/snapshots/" + job_id + "_" + str(count[0]) + ".csv"
    bulk_snapshot_file_name = f"{project_base_dir}/processed/fetcher/snapshots/bulk_" + job_id + "_" + str(
        count[0]) + ".csv"
    fetcher_snapshot_file = open(snapshot_file_name, "w+", newline="", encoding='utf-8-sig')
    fetcher_bulk_snapshot_file = open(bulk_snapshot_file_name, "w+", newline="", encoding='utf-8-sig')
    fetcher_writer = csv.writer(fetcher_snapshot_file)
    fetcher_bulk_writer = csv.writer(fetcher_bulk_snapshot_file)
    competitor_count = 0

    if len(key_link_dict) > 0:
        competitor_count = len(key_link_dict[0].competitor_ranks)
    header_array = get_header_array(competitor_count=competitor_count)
    fetcher_writer.writerow(header_array)
    fetcher_bulk_writer.writerow(header_array)

    if len(key_link_dict) < snapshot_record_count:
        data_list = key_link_dict[0:]
        bulk_data_list = bulk_key_link_dict[0:]

        # removing used elements from lists
        key_link_dict.clear()
        bulk_key_link_dict.clear()
    else:
        data_list = key_link_dict[0:snapshot_record_count]
        bulk_data_list = bulk_key_link_dict[0:snapshot_record_count]

        # removing used elements from lists
        for i in range(snapshot_record_count):
            key_link_dict.remove(key_link_dict[0])
            bulk_key_link_dict.remove(bulk_key_link_dict[0])

    for key_link in data_list:
        for link in key_link.links:
            fetcher_writer.writerow(get_out_row(key_link=key_link, link=link))

    for bulk_key_link in bulk_data_list:
        for link in bulk_key_link.links:
            fetcher_bulk_writer.writerow(get_out_row(key_link=bulk_key_link, link=link))

    fetcher_snapshot_file.close()
    fetcher_bulk_snapshot_file.close()
    return


def write_output_file(job_id: str, keep_snapshots: bool, failure: bool, job_type: str = "fetcher"):
    """ 
    Writes the output file to the output directory by merging snapshots
    :param job_id: The job id
    :param keep_snapshots: flag to remove the snapshot files which are merged to make an output file
    :param failure: flag to return the dataframe instead of csv file
    :param job_type: The job type
    :return: None
    """
    connect_to_socket(node_server_url)
    log = "Merging snapshots to output file..."
    signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
    now = datetime.now()
    time_stamp = now.strftime("%Y%m%d_%H_%M_%S")

    output_file_name = f"{project_base_dir}/processed/fetcher/fetcher_output_" + time_stamp + ".csv"
    bulk_output_file_name = f"{project_base_dir}/processed/fetcher/bulk_fetcher_output_" + time_stamp + ".csv"

    # path to snapshot files
    snapshot_files_path = f"{project_base_dir}/processed/fetcher/snapshots/"

    # creates list with files to merge based on name convention
    snapshot_files_list = [snapshot_files_path + f for f in os.listdir(snapshot_files_path) if f.startswith(job_id)]

    bulk_snapshot_files_list = [snapshot_files_path + f for f in os.listdir(snapshot_files_path) if
                                f.startswith('bulk_' + job_id)]
    snapshot_csv_list = []
    bulk_snapshot_csv_list = []
    count = 0

    # reads each (sorted) file in file_list, converts it to pandas DF and appends it to the csv_list
    for file in sorted(snapshot_files_list):
        count = count + 1
        snapshot_csv_list.append(pd.read_csv(file))

    for file in sorted(bulk_snapshot_files_list):
        bulk_snapshot_csv_list.append(pd.read_csv(file))

    if not snapshot_csv_list or not bulk_snapshot_csv_list:
        return None, None

    # merges single pandas DFs into a single DF, index is refreshed
    snapshot_csv_merged = pd.concat(snapshot_csv_list, ignore_index=True)
    bulk_snapshot_csv_merged = pd.concat(bulk_snapshot_csv_list, ignore_index=True)

    # to return the number of snapshots already taken and dataframe if this function is called by resume route
    if failure == True:
        return count, snapshot_csv_merged

    # sorted the dataframe before making a csv file.
    snapshot_csv_merged.sort_values(by=snapshot_csv_merged.columns[0], ignore_index=True, inplace=True)
    bulk_snapshot_csv_merged.sort_values(by=bulk_snapshot_csv_merged.columns[0], ignore_index=True, inplace=True)

    # Single DF is saved to the path in CSV format, without index column
    snapshot_csv_merged.to_csv(output_file_name, index=False)
    bulk_snapshot_csv_merged.to_csv(bulk_output_file_name, index=False)

    return snapshot_files_path, output_file_name, bulk_output_file_name


def delete_snapshots(snapshot_files_path: str, job_id: str):
    for file in os.listdir(snapshot_files_path):
        if file.startswith(job_id) or file.startswith('bulk_' + job_id):
            os.remove(os.path.join(snapshot_files_path, file))


def get_input_queries(arguments: Arguments, already_searched_keywords_list: List):
    """
    Gets the input queries from the input file
    :param arguments: The arguments object
    :param already_searched_keywords_list: list of keywords that are not needed to be searched again
    :return: A list of input queries (Nodes)
    """
    query_queue = []

    input_keywords_df = read_csv_from_google_cloud(arguments.input_file_path)
    input_keywords_df = input_keywords_df[input_keywords_df['Keyword'].notnull()]

    # returns the dataframe having only those keywords which are not searched before
    input_keywords_df = input_keywords_df[
        ~input_keywords_df['Keyword'].isin(already_searched_keywords_list)].reset_index()

    data_frame_size = len(input_keywords_df)
    i = 0
    query_count = 1
    if 'CPC' in input_keywords_df:
        input_keywords_df['CPC'] = pd.to_numeric(input_keywords_df['CPC'], errors='coerce')
    if 'CPS' in input_keywords_df:
        input_keywords_df['CPS'] = pd.to_numeric(input_keywords_df['CPS'], errors='coerce')
    input_keywords_df['Difficulty'] = pd.to_numeric(input_keywords_df['Difficulty'], errors='coerce')
    while i < data_frame_size:
        query = Node()
        query.keyword = input_keywords_df.iloc[i]["Keyword"]
        if "Volume" in input_keywords_df.iloc[i] and not pd.isna(input_keywords_df.iloc[i]["Volume"]):
            if isinstance(input_keywords_df.iloc[i]["Volume"], str):
                if "-" in input_keywords_df.iloc[i]["Volume"]:
                    split_volume = input_keywords_df.iloc[i]["Volume"].split("-")
                    if split_volume[1].isnumeric():
                        query.volume = float(split_volume[1])
                if input_keywords_df.iloc[i]["Volume"].isnumeric():
                    query.volume = float(input_keywords_df.iloc[i]["Volume"])
            else:
                query.volume = input_keywords_df.iloc[i]["Volume"]
        if "CPC" in input_keywords_df.iloc[i] and not pd.isna(input_keywords_df.iloc[i]["CPC"]):
            query.cpc = input_keywords_df.iloc[i]["CPC"]
        if "CPS" in input_keywords_df.iloc[i] and not pd.isna(input_keywords_df.iloc[i]["CPS"]):
            query.cps = input_keywords_df.iloc[i]["CPS"]
        if "Difficulty" in input_keywords_df.iloc[i] and not pd.isna(input_keywords_df.iloc[i]["Difficulty"]):
            query.difficulty = input_keywords_df.iloc[i]["Difficulty"]
        query_queue.append(query)
        query_count += 1
        i += 1

    cpc_values = [x.cpc for x in query_queue if x.cpc != -1]
    cps_values = [x.cps for x in query_queue if x.cps != -1]
    global cpc_median
    global cps_median
    if len(cpc_values) != 0:
        cpc_median = median(cpc_values)
    if len(cps_values) != 0:
        cps_median = median(cps_values)
    return query_queue
