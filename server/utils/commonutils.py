from typing import List
import re

from utils.models.combinedmodels import Rank
from utils.models.fetchermodels import Feature
from utils.constants.fetcherconstants import CTR_LOOKUP_MATRIX


def get_ctr_values(serp_features: Feature, ranking_position: int = 19):
    """
    This function is used to get the ctr values for the given serp features
    :param serp_features: Feature object containing all the SERP result features
    :param ranking_position:
    :return: current and potential ctr values for the given serp features
    """
    video_flag = serp_features.inline_videos or serp_features.inline_video_carousels
    sitelink_flag = serp_features.sitelinks_search_box or serp_features.sitelinks_expanded
    ctr_index = 0
    if serp_features.organic_result_count > 0:
        ctr_index = 1
        if serp_features.local_results:
            ctr_index = 2
        if serp_features.inline_people_also_search_for:
            ctr_index = 3
        if serp_features.knowledge_graph:
            ctr_index = 4
        if video_flag:
            ctr_index = 5
        if serp_features.answer_box:
            ctr_index = 6
        if serp_features.featured_snippet:
            ctr_index = 7
        if sitelink_flag:
            ctr_index = 8
        if serp_features.top_stories:
            ctr_index = 9
        if serp_features.featured_snippet and serp_features.inline_people_also_search_for:
            ctr_index = 10
        if video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 11
        if serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 12
        if serp_features.knowledge_graph and serp_features.inline_people_also_search_for:
            ctr_index = 13
        if serp_features.knowledge_graph and sitelink_flag:
            ctr_index = 14
        if serp_features.knowledge_graph and video_flag:
            ctr_index = 15
        if serp_features.featured_snippet and video_flag:
            ctr_index = 16
        if video_flag and serp_features.local_results:
            ctr_index = 17
        if serp_features.inline_people_also_search_for and serp_features.answer_box:
            ctr_index = 18
        if sitelink_flag and serp_features.inline_people_also_search_for:
            ctr_index = 19
        if sitelink_flag and video_flag:
            ctr_index = 20
        if sitelink_flag and serp_features.local_results:
            ctr_index = 21
        if video_flag and serp_features.answer_box:
            ctr_index = 22
        if serp_features.top_stories and serp_features.inline_people_also_search_for:
            ctr_index = 23
        if serp_features.inline_people_also_search_for and serp_features.recipes_results:
            ctr_index = 24
        if video_flag and serp_features.recipes_results:
            ctr_index = 25
        if serp_features.knowledge_graph and serp_features.answer_box:
            ctr_index = 26
        if serp_features.featured_snippet and serp_features.local_results:
            ctr_index = 27
        if serp_features.knowledge_graph and serp_features.local_results:
            ctr_index = 28
        if serp_features.featured_snippet and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 29
        if video_flag and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 30
        if sitelink_flag and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 31
        if video_flag and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 32
        if serp_features.knowledge_graph and serp_features.inline_people_also_search_for and serp_features.answer_box:
            ctr_index = 33
        if serp_features.top_stories and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 34
        if serp_features.featured_snippet and serp_features.knowledge_graph and serp_features.inline_people_also_search_for:
            ctr_index = 35
        if serp_features.knowledge_graph and sitelink_flag and serp_features.inline_people_also_search_for:
            ctr_index = 36
        if serp_features.featured_snippet and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 37
        if video_flag and serp_features.inline_people_also_search_for and serp_features.recipes_results:
            ctr_index = 38
        if serp_features.knowledge_graph and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 39
        if sitelink_flag and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 40
        if serp_features.knowledge_graph and serp_features.top_stories and serp_features.inline_people_also_search_for:
            ctr_index = 41
        if serp_features.top_stories and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 42
        if serp_features.knowledge_graph and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 43
        if serp_features.knowledge_graph and sitelink_flag and video_flag:
            ctr_index = 44
        if serp_features.knowledge_graph and video_flag and serp_features.inline_people_also_search_for and serp_features.answer_box:
            ctr_index = 45
        if serp_features.featured_snippet and serp_features.knowledge_graph and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 46
        if serp_features.knowledge_graph and sitelink_flag and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 47
        if serp_features.knowledge_graph and serp_features.top_stories and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 48
        if serp_features.knowledge_graph and sitelink_flag and serp_features.twitter_results and serp_features.inline_people_also_search_for:
            ctr_index = 49
        if serp_features.knowledge_graph and video_flag and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 50
        if serp_features.featured_snippet and video_flag and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 51
        if serp_features.knowledge_graph and sitelink_flag and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 52
        if serp_features.knowledge_graph and video_flag and serp_features.twitter_results and serp_features.inline_people_also_search_for:
            ctr_index = 53
        if serp_features.knowledge_graph and video_flag and serp_features.inline_people_also_search_for and serp_features.recipes_results:
            ctr_index = 54
        if serp_features.top_stories and video_flag and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 55
        if sitelink_flag and serp_features.top_stories and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 56
        if serp_features.knowledge_graph and sitelink_flag and video_flag and serp_features.twitter_results and serp_features.inline_people_also_search_for:
            ctr_index = 57
        if serp_features.knowledge_graph and sitelink_flag and serp_features.top_stories and serp_features.twitter_results and serp_features.inline_people_also_search_for:
            ctr_index = 58
        if serp_features.knowledge_graph and sitelink_flag and serp_features.top_stories and video_flag and serp_features.inline_people_also_search_for:
            ctr_index = 59
        if serp_features.knowledge_graph and sitelink_flag and serp_features.top_stories and serp_features.twitter_results and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 60
        if serp_features.knowledge_graph and sitelink_flag and serp_features.top_stories and video_flag and serp_features.twitter_results and serp_features.inline_people_also_search_for:
            ctr_index = 61
        if serp_features.knowledge_graph and serp_features.top_stories and video_flag and serp_features.twitter_results and serp_features.inline_people_also_search_for and serp_features.local_results:
            ctr_index = 62
    ranking_position_final = ranking_position - 1
    if ranking_position_final < 0 or ranking_position_final > 19:
        ranking_position_final = 19
    return CTR_LOOKUP_MATRIX[ctr_index][ranking_position_final] / 100, CTR_LOOKUP_MATRIX[ctr_index][0] / 100


def remove_permalink(link: str) -> str:
    """
    This function removes the permalink from the given link
    :param link: url to cleanse
    :return: cleansed url
    """
    cleansed_link = link
    if isinstance(link, str) and '#' in link:
        cleansed_link = link.split("#")[0]
    return cleansed_link


def calculate_rank(organic_results: List, answer_box_link: str, target_domain: str, competitor_domains: List[str]):
    """
    This function calculates the rank of the given target and competitor domains in the organic results
    :param organic_results: list of organic results
    :param answer_box_link: answer box link
    :param target_domain: target domain
    :param competitor_domains: list of competitor domains
    :return: list of ranks of the target and competitor domains in the organic results
    :rtype: List[Rank]
    """
    input_domains = competitor_domains[0:]
    input_domains.insert(0, target_domain)
    key_ranks = []
    for domain in input_domains:
        key_rank = Rank()
        if answer_box_link != "" and (f".{domain}" in answer_box_link or f"//{domain}" in answer_box_link):
            key_rank.client_ranking_position = 1
            key_rank.client_url_ranking_count = key_rank.client_url_ranking_count + 1
            key_rank.client_ranking_url = remove_permalink(answer_box_link)
        for result in organic_results:
            if "link" in result and (
                    f".{domain}" in result["link"] or f"//{domain}" in result["link"]):
                if key_rank.client_url_ranking_count == 0:
                    key_rank.client_ranking_position = result["position"]
                    key_rank.client_ranking_url = remove_permalink(result["link"])
                key_rank.client_url_ranking_count = key_rank.client_url_ranking_count + 1
        key_ranks.append(key_rank)
    res = key_ranks[0:]
    return res


def calc_competitor_score(key_link):
    rank_count = 0
    if key_link.rank.client_ranking_position <= 30:
        rank_count += 1
    for rank in key_link.competitor_ranks:
        if rank.client_ranking_position <= 30:
            rank_count += 1
    if rank_count > 11:
        rank_count = 11
    fib_list = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233]
    return fib_list[rank_count], rank_count


def get_fib_helper(rank_position):
    if rank_position <= 3:
        return 13
    if rank_position <= 10:
        return 8
    if rank_position <= 20:
        return 5
    if rank_position <= 30:
        return 3
    if rank_position <= 40:
        return 1
    return 0


def extract_slug(url: str) -> str:
    """
    This function extracts the slug from the given url
    :param url: url to extract slug from
    :return: slug
    """
    if url is None:
        return ""
    url = url.lower()
    url = url.replace("https://", "")
    url = url.replace("http://", "")
    slugs = url.split("/")
    slugs = list(filter(lambda x: x != "", slugs))
    slug = ''
    if len(slugs) > 0:
        slug = slugs[-1]
    slug = slug.replace("-", " ")
    slug = re.sub(r'/blog/|/|\.html|\.php|\.asp', '', slug)
    return slug
