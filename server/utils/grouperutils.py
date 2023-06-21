import csv
from typing import List
from fuzzywuzzy import fuzz

import numpy as np

from utils.commonutils import get_fib_helper, calc_competitor_score, extract_slug
from utils.models.combinedmodels import Rank
from utils.models.groupermodels import Node, Group, Link
from utils.constants.grouperconstants import *
from utils.logutils import *
from utils.socketutils import *
from utils.cloudutils import *
from utils.constants.fetcherconstants import *


def usage(file_name: str):
    """
    Prints the usage of the program
    :param file_name: name of the file
    """
    job_logger.info(f"""Usage: 
    python {file_name} -i <input-file> -o <output-file> -t <threshold> --hard-threshold=<hard-threshold>
    or long arguments ( --input-file, --output-file, --threshold ) can also be used
    """)


def print_error(error_key, code_file: str):
    """
    Prints the error message
    :param error_key: error key
    :param code_file: name of the file
    """
    job_logger.error(f"Argument {error_key} missing!")
    usage(code_file)


def get_args(opts, code_file):
    """
    Gets the arguments from the command line
    :param opts: command line arguments
    :param code_file: name of the file
    :return: arguments
    """
    input_file = output_file = threshold = hard_threshold = None
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage(code_file)
            exit()
        elif opt in ("-i", "--input-file"):
            input_file = arg
        elif opt in ("-o", "--output-file"):
            output_file = arg
        elif opt in ("-t", "--threshold"):
            threshold = int(arg)
        elif opt == "--hard-threshold":
            hard_threshold = int(arg)
        else:
            job_logger.error(f"Unknown option {opt}")
            usage(code_file)
            exit()
    return input_file, output_file, threshold, hard_threshold


def intersect_links(lst1: List[Link], lst2: List[Link]) -> List[str]:
    """
    :param lst1: list to intersect
    :param lst2: second list to intersect
    :return: intersection of the two lists
    """
    intersection = []
    for link1 in lst1:
        for link2 in lst2:
            if link1.url == link2.url:
                intersection.append(link1.url)
                break
    return intersection


def remove_permalink(link: str) -> str:
    """
    :param link: url to cleanse
    :return: cleansed url
    """
    cleansed_link = link.split("#")[0]
    return cleansed_link


def create_group_above_threshold(node_1: Node, node_2: Node, group_number: int, threshold: int):
    """
    Creates a group object above the threshold
    :param node_1: first node
    :param node_2: second node
    :param group_number: group number
    :param threshold: similarity threshold
    :return: group if grouping is possible between node_1 & node_2 else None
    """
    list_intersect = intersect_links(node_1.links, node_2.links)
    intersect_len = len(list_intersect)
    if intersect_len >= threshold and (
            node_2.group is None or intersect_len > node_2.group.links_in_common):
        var_group = Group()
        var_group.main_keyword = node_1.keyword
        var_group.number = group_number
        var_group.common_links = list_intersect
        var_group.links_in_common = intersect_len
        return var_group
    return None


def calc_average_difficulty(filtered_list: List[Node]):
    """
    Calculates the average difficulty of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total = 0
    count = len(filtered_list)
    for key_link in filtered_list:
        total = total + key_link.difficulty
    for key_link in filtered_list:
        key_link.group.average_kw_difficulty = total / count


def calc_average_rank(filtered_list: List[Node]):
    """
    Calculates the average rank of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total = 0
    count = len(filtered_list)
    for key_link in filtered_list:
        total = total + key_link.rank.client_ranking_position
    for key_link in filtered_list:
        key_link.group.average_rank = total / count


def calc_sum_values(filtered_list: List[Node]):
    """
    Calculates the sum of the values of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total = 0
    for key_link in filtered_list:
        total = total + key_link.current_value
    for key_link in filtered_list:
        key_link.group.sum_of_current_values = total


def calc_sum_value_opportunity(filtered_list: List[Node]):
    """
    Calculates the sum of the value opportunities of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total = 0
    for key_link in filtered_list:
        total = total + key_link.value_opportunity
    for key_link in filtered_list:
        key_link.group.sum_value_opportunity = total


def calc_sum_volume_opportunity(filtered_list: List[Node]):
    """
    Calculates the sum of the volume opportunities of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total = 0
    for key_link in filtered_list:
        total = total + key_link.volume_opportunity
    for key_link in filtered_list:
        key_link.group.sum_volume_opportunity = total


def calc_rank_percentage(filtered_list: List[Node]):
    """
    Calculates the rank percentage of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total = 0
    count = len(filtered_list)
    for key_link in filtered_list:
        total = total + key_link.fibonacci_helper
    for key_link in filtered_list:
        key_link.group.rank_percentage = (total / (count * 13)) * 100


def get_total_related_results_count(key_link: Node):
    """
    Calculates the total number of related results
    :param key_link: key link
    :return: total number of related results
    """
    total_related_results_count = 0
    for link in key_link.links:
        total_related_results_count += link.related_results_count
    return total_related_results_count


def calc_topic_volume(filtered_list: List[Node]):
    """
    Calculates the topic volume of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total = 0
    for key_link in filtered_list:
        total = total + key_link.search_volume
    for key_link in filtered_list:
        key_link.group.topic_volume = total


def calc_volume_percentage(filtered_list: List[Node]):
    """
    Calculates the volume percentage of the filtered list
    :param filtered_list: filtered list having the same group
    """
    for key_link in filtered_list:
        if key_link.group.topic_volume > 0:
            key_link.volume_percent = key_link.search_volume / key_link.group.topic_volume


def calc_quartile(filtered_list: List[Node]):
    """
    Calculates the quartile volume of the filtered list
    :param filtered_list: filtered list having the same group
    """
    percent_vol_arr = [x.volume_percent for x in filtered_list]
    qt = np.quantile(percent_vol_arr, 0.75)
    for key_link in filtered_list:
        key_link.group.quartile_volume = qt


def calc_avg_rank_quartile(filtered_list: List[Node]):
    """
    Calculates the average rank quartile of the filtered list
    :param filtered_list: filtered list having the same group
    """
    above_quartile_list = [x for x in filtered_list if x.volume_percent >= x.group.quartile_volume]
    total = 0
    count = len(above_quartile_list)
    for key_link in above_quartile_list:
        total = total + key_link.rank.client_ranking_position
    average_rank_quartile = total / count
    for key_link in filtered_list:
        key_link.group.average_rank_quartile = average_rank_quartile


def calc_priority_score(filtered_list: List[Node]):
    """
    Calculates the priority score of the filtered list
    :param filtered_list: filtered list having the same group
    """
    for key_link in filtered_list:
        ps = key_link.group.rank_percentage * key_link.group.sum_value_opportunity
        ps = ps / key_link.group.average_rank_quartile
        key_link.priority_score = ps


def calc_variant_count(filtered_list: List[Node], vars_count: int):
    """
    Calculates the variant count of the filtered list
    :param filtered_list: filtered list having the same group
    :param vars_count: number of variants in the group
    """
    for key_link in filtered_list:
        key_link.group.variant_count = vars_count


def calc_average_relevancy(filtered_list: List[Node], vars_count: int):
    """
    Calculates the average relevancy of the filtered list
    :param filtered_list: filtered list having the same group
    :param vars_count: number of variants in the group
    """
    total = 0.0
    for key_link in filtered_list:
        total = total + key_link.competitor_score
    average_relevancy = total / vars_count
    for key_link in filtered_list:
        key_link.group.relevancy = average_relevancy


def calc_most_frequent_url(filtered_list: List[Node]) -> str:
    """
    Calculates the most frequent url of the filtered list
    :param filtered_list: filtered list having the same group
    :return: most frequent url
    """
    url_count_dict = dict()
    for key_link in filtered_list:
        if key_link.rank.client_ranking_url != "":
            if key_link.rank.client_ranking_url in url_count_dict:
                url_count_dict[key_link.rank.client_ranking_url] = url_count_dict[key_link.rank.client_ranking_url] + 1
            else:
                url_count_dict[key_link.rank.client_ranking_url] = 1
    most_frequent_url = ""
    most_frequent_url_count = 0
    for url in url_count_dict:
        if url_count_dict[url] > most_frequent_url_count:
            most_frequent_url = url
            most_frequent_url_count = url_count_dict[url]
    return most_frequent_url


def identify_potential_content_gap(filtered_list: List[Node],
                                   key_link_dict: List[Node],
                                   group_number: int,
                                   most_freq_url):
    """
    Identifies the potential content gap of the filtered list
    :param filtered_list: filtered list having the same group
    :param key_link_dict: list of all nodes
    :param group_number: group number
    :param most_freq_url: the most frequent url in the group
    """
    potential_gap_flag = False
    total_keywords = len(key_link_dict)
    i = 0
    while not potential_gap_flag and i < total_keywords:
        if key_link_dict[i].group.number != group_number and \
                key_link_dict[i].rank.client_ranking_position <= 10 and \
                key_link_dict[i].rank.client_ranking_url == most_freq_url:
            potential_gap_flag = True
        i = i + 1
    if potential_gap_flag:
        for key_link in filtered_list:
            key_link.group.potential_content_gap = potential_gap_flag


def identify_total_content_gap(filtered_list: List[Node]):
    """
    Identifies the total content gap of the filtered list
    :param filtered_list: filtered list having the same group
    """
    total_gap_flag = True
    i = 0
    total_keywords = len(filtered_list)
    while total_gap_flag and i < total_keywords:
        if filtered_list[i].rank.client_url_ranking_count > 0:
            total_gap_flag = False
        i += 1
    if not total_gap_flag:
        for key_link in filtered_list:
            key_link.group.total_content_gap = total_gap_flag


def identify_keyword_gap(filtered_list: List[Node]):
    """
    Identifies the keyword gap of the filtered list
    :param filtered_list: filtered list having the same group
    """
    keyword_gap_flag = False
    i = 0
    list_len = len(filtered_list)
    while not keyword_gap_flag and i < list_len:
        if filtered_list[i].rank.client_url_ranking_count == 0 and \
                filtered_list[i].search_volume >= filtered_list[i].group.quartile_volume:
            keyword_gap_flag = True
        i += 1
    if keyword_gap_flag:
        for key_link in filtered_list:
            key_link.group.keyword_gap = keyword_gap_flag


def identify_potential_cannibalisation(filtered_list: List[Node]):
    """
    Identifies the potential cannibalisation of the filtered list
    :param filtered_list: filtered list having the same group
    """
    cannibalization_flag = False
    i = 0
    url_list = set()
    list_len = len(filtered_list)
    while not cannibalization_flag and i < list_len:
        if filtered_list[i].rank.client_ranking_position <= 20:
            if filtered_list[i].rank.client_ranking_url in url_list:
                cannibalization_flag = True
            else:
                url_list.add(filtered_list[i].rank.client_ranking_url)
        i += 1
    if cannibalization_flag:
        for key_link in filtered_list:
            key_link.group.potential_cannibalization = cannibalization_flag


def update_main_keyword(filtered_list: List[Node]):
    """
    Updates the main keyword of the filtered list
    :param filtered_list: filtered list having the same group
    """
    highest_volume = filtered_list[0].search_volume
    highest_volume_keyword = filtered_list[0].keyword
    for key_link in filtered_list:
        if key_link.search_volume > highest_volume:
            highest_volume = key_link.search_volume
            highest_volume_keyword = key_link.keyword
    for key_link in filtered_list:
        key_link.group.main_keyword = highest_volume_keyword


def update_sub_group_topic_volume(filtered_list: List[Node]):
    """
    This function updates the sub group topic volume for provided list with same sub group
    :param filtered_list: list of nodes having same subgroup
    """
    total_sub_group_volume = 0
    for key_link in filtered_list:
        total_sub_group_volume += key_link.search_volume
    for key_link in filtered_list:
        key_link.sub_group.topic_volume = total_sub_group_volume


def calc_sub_group_volume(filtered_list: List[Node]):
    """
    Calculates the sum of volumes of all the elements in one sub group
    :param filtered_list: list of nodes having same group
    """
    total_sub_groups = max([key_link.sub_group.number for key_link in filtered_list])
    for sub_group_num in range(1, total_sub_groups + 1):
        filtered_sub_list = [x for x in filtered_list if x.sub_group.number == sub_group_num]
        vars_count = len(filtered_sub_list)
        if vars_count > 0:
            update_sub_group_topic_volume(filtered_sub_list)


def calc_auto_mapped_url(filtered_list: List[Node], auto_map_dict: dict):
    """
    This function calculates the auto mapped url for the filtered list
    :param filtered_list: list of nodes having same group
    :param auto_map_dict: dictionary containing auto url mappings
    """
    if filtered_list[0].group.number in auto_map_dict:
        for key_link in filtered_list:
            key_link.group.auto_mapped_url = auto_map_dict[key_link.group.number]


def calc_auto_mapped_url_fuzzy(key_link_dict: List[Node], slug_url_dict: dict) -> dict:
    """
    This function calculates the auto mapped url for all keywords
    :param key_link_dict: list containing the key links
    :param slug_url_dict: slug url dictionary
    :return auto_mapped_url_dict: dictionary containing the auto mapped url
    """
    auto_map_dict = dict()
    for slug, url in slug_url_dict.items():
        max_score = fuzzy_match_threshold
        max_score_group = 0
        for key_link in key_link_dict:
            if key_link.keyword == slug and max_score < 101:
                max_score_group = key_link.group.number
                max_score = 101
            else:
                score = fuzz.token_sort_ratio(key_link.keyword, slug)
                if score > max_score:
                    max_score = score
                    max_score_group = key_link.group.number
        if max_score_group != 0:
            auto_map_dict[max_score_group] = url
    return auto_map_dict


def group_nodes(key_link_dict: List[Node], job_id: str, threshold: int = default_grouping_threshold,
                job_type: str = "grouper", progress=10, calc_sub_group=False) -> List[Node]:
    """
    Groups the nodes based on the url similarities considering the threshold
    :param key_link_dict: list of nodes to group
    :param threshold: similarity threshold for intersecting links
    :param job_id: id of the job
    :param job_type: job type
    :param progress: progress of job till now
    :param calc_sub_group: flag if sub group is to be calculated
    :return: list of nodes with groups calculated
    """
    total_keywords = len(key_link_dict)
    connect_to_socket(node_server_url)
    group_number = 1
    progress_step = total_keywords // 10
    increment = 8
    slug_url_dict = dict()
    if calc_sub_group:
        increment = increment // 2
    if job_type == "combined":
        increment = increment // 2
    for i in range(0, total_keywords):
        if key_link_dict[i].rank.client_ranking_url != "":
            client_url = key_link_dict[i].rank.client_ranking_url
            slug_url_dict[extract_slug(client_url)] = client_url
        if key_link_dict[i].group is None:
            if ((group_number - 1) % 1000) == 0:
                log = f"Creating groups {group_number}-{group_number + 999}"
                signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
            group = Group()
            group.number = group_number
            group.common_links = [x.url for x in key_link_dict[i].links]
            group.main_keyword = key_link_dict[i].keyword
            group.links_in_common = len(group.common_links)
            key_link_dict[i].group = group
            for j in range(0, total_keywords):
                group_common = create_group_above_threshold(key_link_dict[i], key_link_dict[j],
                                                            group_number, threshold)
                if group_common is not None:
                    key_link_dict[j].group = group_common
            group_number = group_number + 1
        if i == progress_step:
            progress = progress + increment
            progress_logger.info({"jobId": job_id, "progress": progress})
            progress_step = progress_step + total_keywords // 10

    key_link_dict.sort(key=lambda x: x.group.number)
    log = f"Created {group_number - 1} groups..."
    signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
    auto_map_dict = calc_auto_mapped_url_fuzzy(key_link_dict=key_link_dict, slug_url_dict=slug_url_dict)
    for group_num in range(1, group_number):
        filtered_list = [x for x in key_link_dict if x.group.number == group_num]
        vars_count = len(filtered_list)
        if vars_count > 0:
            calc_average_difficulty(filtered_list=filtered_list)
            calc_average_rank(filtered_list=filtered_list)
            calc_sum_values(filtered_list=filtered_list)
            calc_rank_percentage(filtered_list=filtered_list)
            calc_topic_volume(filtered_list=filtered_list)
            calc_volume_percentage(filtered_list=filtered_list)
            calc_quartile(filtered_list=filtered_list)
            calc_avg_rank_quartile(filtered_list=filtered_list)
            calc_sum_value_opportunity(filtered_list=filtered_list)
            calc_sum_volume_opportunity(filtered_list=filtered_list)
            calc_priority_score(filtered_list=filtered_list)
            calc_variant_count(filtered_list=filtered_list, vars_count=vars_count)
            calc_average_relevancy(filtered_list=filtered_list, vars_count=vars_count)
            if filtered_list[0].group.average_rank_quartile >= 10:
                identify_potential_content_gap(filtered_list=filtered_list,
                                               key_link_dict=key_link_dict,
                                               group_number=filtered_list[0].group.number,
                                               most_freq_url=calc_most_frequent_url(filtered_list=filtered_list))
            identify_total_content_gap(filtered_list=filtered_list)
            identify_keyword_gap(filtered_list=filtered_list)
            identify_potential_cannibalisation(filtered_list=filtered_list)
            calc_auto_mapped_url(filtered_list=filtered_list, auto_map_dict=auto_map_dict)
    key_link_dict.sort(key=lambda x: (x.group.topic_volume, x.search_volume), reverse=True)
    return key_link_dict


def create_sub_group_above_threshold(node_1: Node, node_2: Node, sub_group_number: int, threshold: int,
                                     main_links_above_pth, variant_links_above_pth):
    """
    Creates a sub group if the links in common are above the threshold
    :param node_1: first node
    :param node_2: second node
    :param sub_group_number: sub group number
    :param threshold: threshold for the links in common
    :param main_links_above_pth: main links above threshold
    :param variant_links_above_pth: variant links above threshold
    :return: sub group
    """
    list_intersect = intersect_links(main_links_above_pth, variant_links_above_pth)
    intersect_len = len(list_intersect)
    if intersect_len >= threshold and (
            node_2.sub_group is None or intersect_len > node_2.sub_group.links_in_common):
        var_group = Group()
        var_group.main_keyword = node_1.keyword
        var_group.number = sub_group_number
        var_group.common_links = list_intersect
        var_group.links_in_common = intersect_len
        return var_group
    return None


def sub_group_nodes(key_link_dict: List[Node],
                    job_id: str,
                    threshold: int = default_sub_grouping_threshold,
                    organic_results_count: int = 10,
                    job_type: str = "grouper") -> List[Node]:
    """
    Sub-groups the nodes based on the url similarities considering the threshold
    :param key_link_dict: list of nodes to group
    :param threshold: similarity threshold for intersecting links
    :param job_id: id of the job
    :param job_type: job type
    :param organic_results_count: max position of results to consider
    :return: list of nodes with groups calculated
    """
    total_keywords = len(key_link_dict)
    sub_group_number = 1
    connect_to_socket(node_server_url)
    for i in range(0, total_keywords):
        if key_link_dict[i].sub_group is None:
            main_links_above_pth = [x for x in key_link_dict[i].links if x.position <= organic_results_count]
            sub_group = Group()
            sub_group.number = sub_group_number
            sub_group.common_links = [x.url for x in main_links_above_pth]
            sub_group.main_keyword = key_link_dict[i].keyword
            sub_group.links_in_common = len(sub_group.common_links)
            sub_group.highest_volume = key_link_dict[i].search_volume
            sub_group.highest_volume_keyword = key_link_dict[i].keyword
            key_link_dict[i].sub_group = sub_group
            for j in range(0, total_keywords):
                variant_links_above_pth = [x for x in key_link_dict[j].links if x.position <= organic_results_count]
                common_sub_group = create_sub_group_above_threshold(node_1=key_link_dict[i],
                                                                    node_2=key_link_dict[j],
                                                                    sub_group_number=sub_group_number,
                                                                    threshold=threshold,
                                                                    main_links_above_pth=main_links_above_pth,
                                                                    variant_links_above_pth=variant_links_above_pth)
                if common_sub_group is not None:
                    if key_link_dict[j].search_volume > sub_group.highest_volume:
                        sub_group.highest_volume = key_link_dict[j].search_volume
                        sub_group.highest_volume_keyword = key_link_dict[j].keyword
                    common_sub_group.highest_volume = sub_group.highest_volume
                    common_sub_group.highest_volume_keyword = sub_group.highest_volume_keyword
                    key_link_dict[j].sub_group = common_sub_group
            sub_group_number = sub_group_number + 1
    return key_link_dict


def calc_sub_groups(key_link_dict: List[Node],
                    job_id: str,
                    threshold: int = 6,
                    job_type: str = "grouper",
                    organic_results_count: int = 10) -> List[Node]:
    """
    Calculates the sub groups for the nodes
    :param key_link_dict: list of nodes to group
    :param threshold: similarity threshold for intersecting links
    :param job_id: id of the job
    :param job_type: job type
    :param organic_results_count: max position of result to consider
    :return: list of nodes with groups calculated
    """
    connect_to_socket(node_server_url)
    result = []
    groups = {}
    total_groups = 0
    progress = 50
    if job_type == "combined":
        progress = 70
    for key_link in key_link_dict:
        if key_link.group.number not in groups:
            total_groups = total_groups + 1
            groups[key_link.group.number] = []
        groups[key_link.group.number].append(key_link)
    progress_step = total_groups // 10
    increment = 4
    if job_type == "combined":
        increment = increment // 2
    i = 0
    for group in groups:
        if ((int(group) - 1) % 1000) == 0:
            log = f"Creating sub-groups for group # {int(group)}-{int(group) + 999}"
            signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
        sub_grouping_result = sub_group_nodes(key_link_dict=groups[group], threshold=threshold, job_id=job_id,
                                              job_type=job_type, organic_results_count=organic_results_count)
        calc_sub_group_volume(filtered_list=sub_grouping_result)
        result = result + sub_grouping_result
        i = i + 1
        if i == progress_step:
            progress = progress + increment
            progress_logger.info({"jobId": job_id, "progress": progress})
            progress_step = progress_step + total_groups // 10
    result.sort(key=lambda x: (x.group.topic_volume, x.sub_group.topic_volume, x.search_volume), reverse=True)
    return result


def calculate_rank(key_link: Node, competitor_domains: List[str], target_domain: str):
    """
    Calculates the rank of target and competitor domains for a given keyword
    :param key_link: node to calculate rank for
    :param competitor_domains: list of competitor domains
    :param target_domain: target domain
    :return: rank of target and competitor domains
    """
    input_domains = competitor_domains[0:]
    input_domains.insert(0, target_domain)
    key_ranks = []
    for domain in input_domains:
        key_rank = Rank()
        for result in key_link.links:
            if f".{domain}" in result.url or f"//{domain}" in result.url:
                if key_rank.client_url_ranking_count == 0:
                    key_rank.client_ranking_position = result.position
                    key_rank.client_ranking_url = result.url
                key_rank.client_url_ranking_count = key_rank.client_url_ranking_count + 1
                if 1 <= key_rank.client_ranking_position <= 20:
                    key_rank.current_traffic = CTR_LOOKUP[
                                                   key_rank.client_ranking_position - 1] * key_link.search_volume * key_link.cps
                else:
                    key_rank.current_traffic = CTR_LOOKUP[19] * key_link.search_volume * key_link.cps
                key_rank.current_value = key_rank.current_traffic * key_link.cpc
        key_ranks.append(key_rank)
    return key_ranks[0:]


def read_input(input_file_path: str,
               job_type: str = "grouper",
               calc_rank: bool = False,
               target_domain: str = "", organic_results_count: int = 10,
               competitor_domains: List[str] = []) -> List[Node]:
    """
    Reads the input file and returns the nodes
    :param input_file_path: path to input file
    :param job_type: job type
    :param calc_rank: flag to check if rank is to be calculated
    :param target_domain: target domain
    :param organic_results_count: max position of result to consider
    :param competitor_domains: list of competitor domains
    :return: list of nodes
    """
    key_link_dict = []
    if job_type == "grouper":
        df = read_csv_from_google_cloud(input_file_path)
    else:
        df = pd.read_csv(filepath_or_buffer=input_file_path, encoding='utf-8-sig')
    df = df[df["Keyword"].notnull() & df["Link"].notnull() & df["Volume"].notnull()]
    df.sort_values(by=["Volume", "Keyword"], inplace=True, ascending=False)
    data_frame_size = len(df)
    i = 0
    comp_rank_headers = list(
        filter(lambda item: 'Competitor' in item and 'Score' not in item and 'count' not in item, df.columns.to_list()))
    while i < data_frame_size:
        if not isinstance(df.iloc[i]["Keyword"], str):
            job_logger.error("invalid type exist for keyword")
        if not isinstance(df.iloc[i]["Volume"], np.int64) and not isinstance(df.iloc[i]["Volume"], np.float64):
            job_logger.error("invalid type exist for volume")
        keyword = df.iloc[i]["Keyword"]
        key_link = Node()
        rank = Rank()
        key_link.keyword = keyword
        key_link.search_volume = df.iloc[i]["Volume"]
        if CLIENT_RANKING_URL in df.iloc[i] and not pd.isna(df.iloc[i][CLIENT_RANKING_URL]):
            rank.client_ranking_url = df.iloc[i][CLIENT_RANKING_URL]
        if CLIENT_RANKING_POSITION in df.iloc[i] and not pd.isna(df.iloc[i][CLIENT_RANKING_POSITION]):
            rank.client_ranking_position = df.iloc[i][CLIENT_RANKING_POSITION]
        if CLIENT_URL_RANKING_COUNT in df.iloc[i] and not pd.isna(df.iloc[i][CLIENT_URL_RANKING_COUNT]):
            rank.client_url_ranking_count = df.iloc[i][CLIENT_URL_RANKING_COUNT]
        key_link.rank = rank

        if "Difficulty" in df.iloc[i] and not pd.isna(df.iloc[i]["Difficulty"]):
            if isinstance(df.iloc[i]["Difficulty"], np.float64) or isinstance(
                    df.iloc[i]["Difficulty"], np.int64):
                key_link.difficulty = df.iloc[i]["Difficulty"]
            else:
                job_logger.error("invalid type difficulty")
        if "Current Traffic" in df.iloc[i] and not pd.isna(df.iloc[i]["Current Traffic"]):
            if isinstance(df.iloc[i]["Current Traffic"], np.float64) or isinstance(
                    df.iloc[i]["Current Traffic"], np.int64):
                key_link.current_traffic = df.iloc[i]["Current Traffic"]
            else:
                job_logger.error("invalid type current traffic")
        if "Potential Traffic" in df.iloc[i] and not pd.isna(df.iloc[i]["Potential Traffic"]):
            if isinstance(df.iloc[i]["Potential Traffic"], np.float64) or isinstance(
                    df.iloc[i]["Potential Traffic"], np.int64):
                key_link.potential_traffic = df.iloc[i]["Potential Traffic"]
            else:
                job_logger.error("invalid type potential traffic")
        if "Current Value" in df.iloc[i] and not pd.isna(df.iloc[i]["Current Value"]):
            if isinstance(df.iloc[i]["Current Value"], np.float64) or isinstance(
                    df.iloc[i]["Current Value"], np.int64):
                key_link.current_value = df.iloc[i]["Current Value"]
            else:
                job_logger.error("invalid type current value")
        if "Potential Value" in df.iloc[i] and not pd.isna(df.iloc[i]["Potential Value"]):
            if isinstance(df.iloc[i]["Potential Value"], np.float64) or isinstance(
                    df.iloc[i]["Potential Value"], np.int64):
                key_link.potential_value = df.iloc[i]["Potential Value"]
            else:
                job_logger.error("invalid type potential value")
        if "Fibonacci Helper" in df.iloc[i] and not pd.isna(df.iloc[i]["Fibonacci Helper"]):
            if isinstance(df.iloc[i]["Fibonacci Helper"], np.float64) or isinstance(
                    df.iloc[i]["Fibonacci Helper"], np.int64):
                key_link.fibonacci_helper = df.iloc[i]["Fibonacci Helper"]
            else:
                job_logger.error("invalid type fibonacci helper")
        if "Value Opportunity" in df.iloc[i] and not pd.isna(df.iloc[i]["Value Opportunity"]):
            if isinstance(df.iloc[i]["Value Opportunity"], np.float64) or isinstance(
                    df.iloc[i]["Value Opportunity"], np.int64):
                key_link.value_opportunity = df.iloc[i]["Value Opportunity"]
            else:
                job_logger.error("invalid type value opportunity")
        if "Volume Opportunity" in df.iloc[i] and not pd.isna(df.iloc[i]["Volume Opportunity"]):
            if isinstance(df.iloc[i]["Volume Opportunity"], np.float64) or isinstance(
                    df.iloc[i]["Volume Opportunity"], np.int64):
                key_link.volume_opportunity = df.iloc[i]["Volume Opportunity"]
            else:
                job_logger.error("invalid type volume opportunity")
        if "CPC" in df.iloc[i] and not pd.isna(df.iloc[i]["CPC"]):
            if isinstance(df.iloc[i]["CPC"], np.float64) or isinstance(
                    df.iloc[i]["CPC"], np.int64):
                key_link.cpc = df.iloc[i]["CPC"]
            else:
                job_logger.error("invalid type CPC")
        if "CPS" in df.iloc[i] and not pd.isna(df.iloc[i]["CPS"]):
            if isinstance(df.iloc[i]["CPS"], np.float64) or isinstance(
                    df.iloc[i]["CPS"], np.int64):
                key_link.cps = df.iloc[i]["CPS"]
            else:
                job_logger.error("invalid type CPS")

        if "Primary Intents" in df.iloc[i] and not pd.isna(df.iloc[i]["Primary Intents"]):
            if isinstance(df.iloc[i]["Primary Intents"], str):
                key_link.primary_search_intents = df.iloc[i]["Primary Intents"]
            else:
                job_logger.error("invalid type primary intents")
        if "Secondary Intents" in df.iloc[i] and not pd.isna(df.iloc[i]["Secondary Intents"]):
            if isinstance(df.iloc[i]["Secondary Intents"], str):
                key_link.secondary_search_intents = df.iloc[i]["Secondary Intents"]
            else:
                job_logger.error("invalid type secondary intents")
        if not isinstance(calc_rank, bool):
            job_logger.error("invalid type calc_rank")

        links = []
        j = i
        while j < data_frame_size and df.iloc[j]["Keyword"] == keyword:
            link = Link()
            if isinstance(df.iloc[j]["Link"], str):
                link.url = remove_permalink(df.iloc[j]["Link"])
            else:
                job_logger.error("invalid type of link exist")
            if isinstance(df.iloc[j]["Position"], np.int64):
                link.position = df.iloc[j]["Position"]
            else:
                job_logger.error("invalid type of position exist")
            if "Related Results Count" in df.iloc[j]:
                if isinstance(df.iloc[j]["Related Results Count"], np.int64):
                    link.related_results_count = df.iloc[j]["Related Results Count"]
                else:
                    job_logger.error("invalid type of related result count exist")
            links.append(link)
            j = j + 1
        key_link.links = links
        comp_ranks = []
        total_competitors = len(comp_rank_headers)
        for k in range(0, total_competitors, 4):
            comp_rank = Rank()
            if comp_rank_headers[k] in df.iloc[i] and not pd.isna(df.iloc[i][comp_rank_headers[k]]):
                comp_rank.client_ranking_url = df.iloc[i][comp_rank_headers[k]]
            if k + 1 < total_competitors and comp_rank_headers[k + 1] in df.iloc[i] and not pd.isna(
                    df.iloc[i][comp_rank_headers[k + 1]]):
                comp_rank.client_ranking_position = df.iloc[i][comp_rank_headers[k + 1]]
            if k + 2 < total_competitors and comp_rank_headers[k + 2] in df.iloc[i] and not pd.isna(
                    df.iloc[i][comp_rank_headers[k + 2]]):
                comp_rank.current_traffic = df.iloc[i][comp_rank_headers[k + 2]]
            if k + 3 < total_competitors and comp_rank_headers[k + 3] in df.iloc[i] and not pd.isna(
                    df.iloc[i][comp_rank_headers[k + 3]]):
                comp_rank.current_value = df.iloc[i][comp_rank_headers[k + 3]]
            comp_ranks.append(comp_rank)
        key_link.competitor_ranks = comp_ranks
        if 'Competitor Score' in df.iloc[i]:
            key_link.competitor_score = df.iloc[i]['Competitor Score']
        if 'Competitor ranking count' in df.iloc[i]:
            key_link.competitor_ranking_count = df.iloc[i]['Competitor ranking count']
        if calc_rank:
            competitor_ranks = calculate_rank(key_link=key_link,
                                              competitor_domains=competitor_domains,
                                              target_domain=target_domain)
            key_link.rank = competitor_ranks[0]
            key_link.competitor_ranks = competitor_ranks[1:]
            key_link.links = key_link.links[:20]

            potential_traffic = CTR_LOOKUP[0] * key_link.search_volume
            if 1 <= key_link.rank.client_ranking_position <= 20:
                current_traffic = CTR_LOOKUP[
                                      key_link.rank.client_ranking_position - 1] * key_link.search_volume * key_link.cps
            else:
                current_traffic = CTR_LOOKUP[19] * key_link.search_volume * key_link.cps

            current_value = current_traffic * key_link.cpc
            potential_value = potential_traffic * key_link.cpc
            key_link.current_traffic = current_traffic
            key_link.current_value = current_value
            key_link.potential_value = potential_value
            key_link.potential_traffic = potential_traffic
            key_link.value_opportunity = key_link.potential_value - key_link.current_value
            key_link.volume_opportunity = key_link.search_volume - key_link.current_traffic
            key_link.fibonacci_helper = get_fib_helper(key_link.rank.client_ranking_position)
            comp_score, comp_count = calc_competitor_score(key_link=key_link)
            key_link.competitor_score = comp_score
            key_link.competitor_ranking_count = comp_count

        # Consider organic results within Position Threshold only
        key_link.links = [x for x in key_link.links if x.position <= organic_results_count]

        key_link_dict.append(key_link)
        i = j
    return key_link_dict


def assign_clusters(key_link_dict: List[Node], mkw_ctr_dict: dict):
    """
    Assigns clusters to the nodes in the graph
    :param key_link_dict: List of nodes in the graph
    :param mkw_ctr_dict: Dictionary of MKW CTRs
    """
    for key_link in key_link_dict:
        key_link.group.cluster = mkw_ctr_dict[key_link.group.main_keyword]
    # key_link_dict.sort(key=lambda x: x.group.cluster)


def get_header_array(contains_sub_groups: bool, competitor_count: int):
    """
    Returns the header array for the output CSV
    :param contains_sub_groups: Whether to include sub groups in output
    :param competitor_count: Number of competitors
    :return: Header array
    """
    header_array = group_header_array[0:]
    if contains_sub_groups:
        header_array = sub_group_header_array[0:]
    header_array.append('Competitor Score')
    header_array.append('Relevancy')
    header_array.append('Competitor ranking count')
    header_array.append('Related results count')
    competitor_number = 'A'
    for i in range(competitor_count):
        header_array.append(f'Competitor {competitor_number} ranking URL')
        header_array.append(f'Competitor {competitor_number} rank')
        header_array.append(f'Competitor {competitor_number} current traffic')
        header_array.append(f'Competitor {competitor_number} current value')
        competitor_number = chr(ord(competitor_number) + 1)
    return header_array


def get_out_row(key_link: Node, contains_sub_groups: bool, mkw_ctr_dict: dict, highest_vol_mkw_arr):
    """
    Returns the output row for the output CSV
    :param key_link: Node to get output row for
    :param contains_sub_groups: Whether to include sub groups in output
    :param mkw_ctr_dict: Dictionary of MKW CTRs
    :param highest_vol_mkw_arr: Array of highest volume MKW
    :return: Output row
    """
    out_row = [key_link.group.number,
               key_link.group.main_keyword,
               key_link.keyword, key_link.group.links_in_common,
               key_link.search_volume, key_link.primary_search_intents, key_link.secondary_search_intents,
               key_link.rank.client_ranking_url,
               key_link.rank.client_ranking_position, key_link.rank.client_url_ranking_count,
               round(key_link.cpc, 2),
               key_link.group.variant_count,
               round(key_link.difficulty, 2),
               round(key_link.group.average_kw_difficulty, 2),
               round(key_link.current_traffic, 2),
               round(key_link.potential_traffic, 2),
               round(key_link.current_value, 2),
               round(key_link.potential_value, 2),
               round(key_link.potential_value - key_link.current_value, 2),
               round(key_link.group.average_rank, 2),
               round(key_link.group.sum_of_current_values, 2),
               round(key_link.group.rank_percentage, 2),
               round(key_link.group.sum_value_opportunity, 2),
               round(key_link.group.sum_volume_opportunity, 2),
               round(key_link.group.topic_volume, 2),
               round(key_link.volume_percent, 2),
               round(key_link.group.quartile_volume, 2),
               round(key_link.group.average_rank_quartile, 2),
               round(key_link.priority_score, 2),
               key_link.group.auto_mapped_url,
               key_link.group.potential_content_gap,
               key_link.group.total_content_gap,
               key_link.group.keyword_gap,
               key_link.group.potential_cannibalization

               ]
    if contains_sub_groups:
        out_row = [key_link.group.number,
                   key_link.group.main_keyword,
                   key_link.keyword, key_link.group.links_in_common,
                   key_link.sub_group.highest_volume_keyword, key_link.search_volume, key_link.primary_search_intents,
                   key_link.secondary_search_intents, key_link.rank.client_ranking_url,
                   key_link.rank.client_ranking_position, key_link.rank.client_url_ranking_count,
                   round(key_link.cpc, 2),
                   key_link.group.variant_count,
                   round(key_link.difficulty, 2),
                   round(key_link.group.average_kw_difficulty, 2),
                   round(key_link.current_traffic, 2),
                   round(key_link.potential_traffic, 2),
                   round(key_link.current_value, 2),
                   round(key_link.potential_value, 2),
                   round(key_link.potential_value - key_link.current_value, 2),
                   round(key_link.group.average_rank, 2),
                   round(key_link.group.sum_of_current_values, 2),
                   round(key_link.group.rank_percentage, 2),
                   round(key_link.group.sum_value_opportunity, 2),
                   round(key_link.group.sum_volume_opportunity, 2),
                   round(key_link.group.topic_volume, 2),
                   round(key_link.volume_percent, 2),
                   round(key_link.group.quartile_volume, 2),
                   round(key_link.group.average_rank_quartile, 2),
                   round(key_link.priority_score, 2),
                   round(key_link.sub_group.topic_volume, 2),
                   key_link.group.auto_mapped_url,
                   key_link.group.potential_content_gap,
                   key_link.group.total_content_gap,
                   key_link.group.keyword_gap,
                   key_link.group.potential_cannibalization
                   ]
    out_row.append(key_link.competitor_score)
    out_row.append(round(key_link.group.relevancy, 2))
    out_row.append(key_link.competitor_ranking_count)
    out_row.append(get_total_related_results_count(key_link=key_link))
    for comp_rank in key_link.competitor_ranks:
        out_row.append(comp_rank.client_ranking_url)
        out_row.append(comp_rank.client_ranking_position)
        out_row.append(round(comp_rank.current_traffic, 2))
        out_row.append(round(comp_rank.current_value, 2))
    return out_row


def write_results(output_file_path: str, job_id: str,
                  key_link_dict: List[Node],
                  mkw_ctr_dict: dict = None,
                  highest_vol_mkw_arr=[],
                  contains_sub_groups: bool = False,
                  job_type: str = "grouper"):
    """
    Writes the results to a csv file
    :param output_file_path: file to write output to
    :param key_link_dict: keywords to write
    :param mkw_ctr_dict: dictionary containing cluster information
    :param highest_vol_mkw_arr: array of highest volume keywords
    :param contains_sub_groups: if the output is to contain sub groups
    :param job_id: id of the job
    :param job_type: job type
    :return: None
    """

    connect_to_socket(node_server_url)
    if not isinstance(output_file_path, str):
        job_logger.error("invalid type output_file_path")
    group_output_file = open(output_file_path, "w+", newline="", encoding='utf-8-sig')
    log = f"Writing results to output file..."
    signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
    group_writer = csv.writer(group_output_file)
    competitor_count = 0
    if len(key_link_dict) > 0:
        competitor_count = len(key_link_dict[0].competitor_ranks)
    header_array = get_header_array(contains_sub_groups=contains_sub_groups, competitor_count=competitor_count)
    group_writer.writerow(header_array)
    for key_link in key_link_dict:
        group_writer.writerow(
            get_out_row(key_link=key_link, contains_sub_groups=contains_sub_groups, mkw_ctr_dict=mkw_ctr_dict,
                        highest_vol_mkw_arr=highest_vol_mkw_arr))

    log = "Finished writing results..."
    signal_logger.info({"jobId": job_id, "type": job_type, "log": f"[{get_time_stamp()}] {log}"})
    group_output_file.close()
