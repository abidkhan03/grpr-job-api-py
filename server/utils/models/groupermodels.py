from typing import List, Optional

from utils.models.combinedmodels import Rank


class Group:
    number: int = 0
    common_links: List[str] = []
    links_in_common: int = 0
    main_keyword: str = ""
    highest_volume: int = 0
    highest_volume_keyword: str = ""
    average_kw_difficulty: float = 0.0
    average_rank: float = 101
    sum_of_current_values: float = 0.0
    rank_percentage: float = 0
    topic_volume: int = 0
    quartile_volume: float = 0.0
    average_rank_quartile: float = 0.0
    sum_value_opportunity: float = 0.0
    sum_volume_opportunity: float = 0.0
    variant_count: int = 0
    relevancy: float = 1.0
    cluster: int = 0
    potential_content_gap: bool = False
    total_content_gap: bool = True
    keyword_gap: bool = False
    potential_cannibalization: bool = False
    auto_mapped_url: str = ""


class Link:
    url: str = ""
    position: int = 0
    related_results_count: int = 0


class Node:
    keyword: str = ""
    links: List[Link] = []
    search_volume: int = 0
    group: Optional[Group] = None
    sub_group: Optional[Group] = None
    primary_search_intents: str = ""
    secondary_search_intents: str = ""
    rank: Rank = Rank()
    cpc: float = 0.0
    cps: float = 0.0
    difficulty: float = 0.0
    current_traffic: float = 0.0
    potential_traffic: float = 0.0
    current_value: float = 0.0
    potential_value: float = 0.0
    fibonacci_helper: int = 0
    volume_percent: float = 0.0
    priority_score: float = 0.0
    value_opportunity: float = 0.0
    volume_opportunity: float = 0.0
    competitor_ranks: List[Rank] = []
    competitor_score: int = 1
    competitor_ranking_count: int = 0
