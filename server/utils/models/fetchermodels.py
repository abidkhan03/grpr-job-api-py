from typing import List
from utils.constants.fetcherconstants import *
from utils.models.combinedmodels import Rank


class Feature:
    answer_box: bool = False
    organic_result_count: int = 0
    ad_result_top_count: int = 0
    ad_result_bottom_count: int = 0
    ad_result_right_count: int = 0
    featured_snippet: bool = False
    sitelinks_search_box: bool = False
    sitelinks_expanded: bool = False
    sitelinks_inline: bool = False
    events_results: bool = False
    inline_images: bool = False
    inline_people_also_search_for: bool = False
    shopping_results: bool = False
    inline_videos: bool = False
    inline_video_carousels: bool = False
    knowledge_graph: bool = False
    local_results: bool = False
    news_results: bool = False
    top_stories: bool = False
    inline_products: bool = False
    recipes_results: bool = False
    related_questions: bool = False
    twitter_results: bool = False


class Link:
    url: str = ""
    position: int = 0
    title: str = ""
    snippet: str = ""
    related_results_count: int = 0


class Node:
    keyword: str = ""
    volume: int = 5
    links: List[Link] = []
    primary_search_intents: List[str] = []
    secondary_search_intents: List[str] = []
    rank: Rank = Rank()
    competitor_ranks: List[Rank] = []
    difficulty: float = 0.0
    cpc: float = -1.0
    cps: float = -1.0
    current_traffic: float = 0.0
    potential_traffic: float = 0.0
    current_value: float = 0.0
    potential_value: float = 0.0
    fibonacci_helper: int = 0


class Arguments:
    input_file_path: str = ""
    api_key: str = ""
    search_engine: str = SEARCH_ENGINE
    region: str = REGION
    gl: str = GL
