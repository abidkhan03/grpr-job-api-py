# Keyword Grouper App

## Basic Idea

To be able to process a set of keywords and group them together. This grouping is performed on the basis of similarities in the _links_ in results that are returned from a search engine.

## Types of Jobs

There are three main types of jobs

- Fetcher
- Grouper
- Combined

## Fetcher

### Input format

This type of job requires an input CSV file with the following mandatory columns

```
Keyword, Difficulty, Volume, CPC, CPS
```

### Tasks

Fetcher performs the following tasks

- Use `SERPAPI` to get the top 100 results from the provided search engine.
- Calculate the ranking information (_ranking_url, ranking_position and ranking_count_) of provided domains.
- Calculate traffic information fields like _current traffic, potential traffic, current volume and potential volume_.
- Upload the output file to _Google Cloud Storage_ and notify the front-end that the job has been completed (using sockets)

### Output format

Fetcher job outputs a CSV file having the following columns

```
Keyword	Volume,	Link, Position, Title, Snippet, Primary Intents, Secondary Intents, Client Ranking URL, Client Ranking Position, Client URL Ranking Count, CPC, CPS, Difficulty, Current Traffic, Potential Traffic, Current Value, Potential Value, Fibonacci Helper, Value Opportunity, Volume Opportunity, Competitor Score, Competitor ranking count, Related Results Count
```

Optional fields

```
Competitor A ranking URL, Competitor A rank, Competitor A current traffic, Competitor A current value, Competitor B ranking URL, ... (depending on how many competitor domains are provided)
```

## Grouper

### Input format

Grouper job accepts a CSV having the same structure as _fetcher's output_.

### Tasks

Grouper performs the following tasks

- Create groups on the basis of similarities in links i.e _keywords having >= threshold common links are placed in one group_
- Calculate some extra fields like _rank percentage, priority score, content gap, keyword gap and potential cannibalization_
- Upload the output file to google cloud storage.
- Upload the output to the Snowflake database using SQL queries
- Notify the front-end that the job has been completed (using sockets)

### Output format

Grouper job outputs a CSV file having the following columns

```
ID, Main Keyword, Variation, Common Links, Volume, Primary Intents, Secondary Intents, Client Ranking URL, Client Ranking Position, Client URL Ranking Count, CPC, Variant Count, Difficulty, Average MK KD, Current Traffic, Potential Traffic, Current Value, Potential Value, Value Delta, Average Rank, Sum Values, Percentage Ranking, Value Opportunity, Volume Opportunity, Topic Volume, Percentage Volume, Quartile of Volume, Avg Rank Quartile 4, Priority Score, Potential Content Gap, Total Content Gap, Keyword Gap, Potential Cannibalization, Competitor Score, Relevancy, Competitor ranking count, Related results count
```

Optional fields

```
Sub Group
```

## Combined

The combined job is fetcher and grouper combined. Given an input same as a fetcher, this job performs first fetcher tasks and then grouper tasks.

### Input format

The combined job accepts a CSV having the same structure as _fetcher's input_.

### Tasks

Combined job performs following tasks

- Run a fetcher job
- Upload fetcher's output to google cloud storage
- Run grouper job by passing it the fetcher's output
- Upload the grouper's output to google cloud storage.
- Upload the grouper's output to Snowflake database using SQL queries
- Notify the front-end that the job has been completed (using sockets)

### Output format

The combined job's final output is the same as _grouper's output_.
