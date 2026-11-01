<h1 align="center">Distributed Multi-Model Analytics for E-commerce Data</h1>



<p align="center">

Document and wide-column data models, batch processing, Spark SQL, an integrated customer lifetime value analysis,<br>

visualisation and recommendations for 100,000 sessions and 30,000 transactions of an online shop,<br>

built with MongoDB, HBase and Apache Spark.

</p>



<p align="center">

&#x20; <img alt="MongoDB" src="https://img.shields.io/badge/MongoDB-8.2.4-47A248?logo=mongodb\&logoColor=white">

&#x20; <img alt="HBase" src="https://img.shields.io/badge/HBase-2.1.3-D22128">

&#x20; <img alt="Apache Spark" src="https://img.shields.io/badge/Apache%20Spark-4.2.0-E25A1C?logo=apachespark\&logoColor=white">

&#x20; <img alt="Python" src="https://img.shields.io/badge/Python-3.13-3776AB?logo=python\&logoColor=white">

&#x20; <img alt="Java" src="https://img.shields.io/badge/Java-17-ED8B00?logo=openjdk\&logoColor=white">

&#x20; <img alt="Docker" src="https://img.shields.io/badge/Docker-29.8-2496ED?logo=docker\&logoColor=white">

&#x20; <img alt="Platform" src="https://img.shields.io/badge/Platform-Windows%2011-0078D4?logo=windows11\&logoColor=white">

</p>



| | |

|---|---|

| \*\*Course\*\* | Big Data Analytics, Adventist University of Central Africa (AUCA) |

| \*\*Project\*\* | Final project (individual), issued 28 October 2026, due 11 November 2026 |

| \*\*Student\*\* | Erneste UWIMPUHWE, ID 101414 |

| \*\*Repository\*\* | `101414\_BDA\_FINAL\_EXAM\_PROJECT\_SUBMISSION` |



\---



\## Table of contents



1\. \[Overview](#overview)

2\. \[Key findings](#key-findings)

3\. \[Architecture](#architecture)

4\. \[Data and environment](#data-and-environment)

5\. \[Repository structure](#repository-structure)

6\. \[How to reproduce](#how-to-reproduce)

7\. \[Results](#results)

8\. \[Design decisions](#design-decisions)

9\. \[Limitations](#limitations)

10\. \[Evidence and report](#evidence-and-report)



\---



\## Overview



An online shop wants its analysts to understand how customers browse, buy and are worth over time, and to choose the right data technology for each question. This project stores the same e-commerce data in \*\*MongoDB\*\* and \*\*HBase\*\*, processes it with \*\*Apache Spark\*\*, combines the stores in one customer lifetime value (CLV) analysis, and turns the results into charts and recommendations.



| Part | Content |

|---|---|

| 1. Data modelling | MongoDB document model with three aggregation pipelines; HBase tables with a reversed-timestamp row key |

| 2. Spark processing | Cleaning, products viewed or bought together, cohort analysis and four Spark SQL queries |

| 3. Integration | CLV estimation joining MongoDB purchase metrics with HBase session metrics in Spark |

| 4. Visualisation | Funnel, weekly revenue, customer-tier and category charts with business insights |



\## Key findings



\- \*\*Same data, same answers.\*\* Category revenue and top-product units are identical in MongoDB and Spark, and the embedded `purchase\_summary` matches the aggregation pipeline for every user (0 mismatches).

\- \*\*Revenue is not profit.\*\* The highest-revenue category ranks 15th in estimated profit; the 17th by revenue ranks first.

\- \*\*Funnel.\*\* Only 20.4% of sessions end in a purchase; the largest loss is between cart and checkout, where 44.8% of carts continue.

\- \*\*HBase row key.\*\* `user\_id` plus a reversed timestamp returns a user's newest sessions first, with no sorting.

\- \*\*Customer value.\*\* Platinum customers are worth 6.2 times Bronze customers over 12 months but browse only 1.12 times as much; 76 heavy browsers have never bought.



\## Architecture



```mermaid

flowchart LR

&#x20;   G\["dataset\_generator.py"] --> J\[("JSON files")]

&#x20;   J --> M\[("MongoDB<br/>documents")]

&#x20;   J --> H\[("HBase<br/>wide-column")]

&#x20;   J --> S\["Apache Spark<br/>clean, SQL, cohorts"]

&#x20;   M -- "extract (CSV)" --> S

&#x20;   H -- "extract (CSV)" --> S

&#x20;   S --> V\["Charts and insights"]

```



| Store | Holds | Why |

|---|---|---|

| \*\*MongoDB\*\* | catalog, user profiles, transactions, nested session journeys | nested documents and flexible aggregation |

| \*\*HBase\*\* | per-session event rows, per-product daily metrics | key-ordered time-series scans, newest session first |

| \*\*Spark\*\* | cleaned Parquet tables and the join across systems | batch processing, SQL, cross-system joins |



\## Data and environment



| Item | Value |

|---|---|

| Dataset | 10,000 users, 5,000 products, 25 categories, 100,000 sessions, 30,000 transactions, about 90 days (August to October 2026) |

| Generator | supplied `dataset\_generator.py` (a Colab export; its code cell was extracted and the volumes set for an 8 GB laptop) |

| Machine | Windows 11 Pro, 8,035 MB RAM, so heavy tools run one at a time |

| MongoDB | 8.2.4, own instance on port 27018, data on drive D: |

| HBase | 2.1.3 in Docker (`harisekhon/hbase:2.1`), 1.8 GB memory limit |

| Spark | 4.2.0 (Scala 2.13.18), local mode, Java 17, Python 3.13.6 |



\## Repository structure



```

.

├── data\_generation/   dataset generator (100,000 sessions, 30,000 transactions)

├── mongodb/           loader and the three aggregation pipelines

├── hbase/             table creation, loader, shell and Python queries

├── spark/             JSON Lines conversion, cleaning, analysis, Spark SQL, chart data

├── integration/       MongoDB and HBase extracts and the CLV job

├── visuals/           make\_charts.py and the generated charts

├── results/           CSV output of every analysis

├── screenshots/       evidence for each step

├── requirements.txt   Python dependencies

└── README.md

```



\## How to reproduce



Work in `D:\\BDA\_Project`. Memory is tight on 8 GB, so run one heavy tool at a time.



<details>

<summary><b>Show the commands in execution order</b></summary>



```cmd

python -m venv venv

venv\\Scripts\\activate

pip install -r requirements.txt



:: 1. data

cd data

python dataset\_generator.py



:: 2. MongoDB (own instance, data on D:)

mongod --dbpath D:\\BDA\_Project\\mongo\\data --port 27018 --bind\_ip 127.0.0.1 --wiredTigerCacheSizeGB 0.5

python mongo\\load\_mongo.py

python mongo\\mongo\_aggregations.py



:: 3. HBase (Docker)

docker pull harisekhon/hbase:2.1

docker run -d --name hbase --memory 1800m -p 2181:2181 -p 9090:9090 -p 16010:16010 -v D:\\BDA\_Project\\hbase:/work harisekhon/hbase:2.1

docker exec hbase hbase shell /work/create\_tables.hbase

python hbase\\hbase\_load.py

python hbase\\hbase\_queries.py

docker exec hbase hbase shell /work/hbase\_shell\_queries.hbase



:: 4. Spark (stop Docker first to free memory)

python spark\\prep\_sessions\_jsonl.py

python spark\\spark\_clean.py

python spark\\spark\_analysis.py

python spark\\spark\_sql.py



:: 5. Integrated query: extract from each store, then join in Spark

python integration\\export\_from\_mongo.py      :: mongod running

python integration\\export\_from\_hbase.py      :: hbase container running

python integration\\spark\_clv.py              :: Docker and mongod stopped



:: 6. Charts

python spark\\spark\_viz\_data.py

python visuals\\make\_charts.py

```



</details>



\## Results



| Area | Result |

|---|---|

| MongoDB top seller | `prod\_01476`, 61 units (also confirmed by Spark) |

| MongoDB revenue by category | the top revenue category ranks 15th by estimated profit; the 17th by revenue ranks 1st |

| MongoDB segmentation | 4,962 regular buyers (3 to 5 orders) produce about 62% of total spend |

| HBase | 100,000 session rows; newest-first prefix scan per user; 255,199 product-day metric rows |

| Spark cleaning | 5 tables typed, de-duplicated and validated; sessions flattened into page views and cart items |

| Spark pairs and cohorts | lift is unreliable at very low support (2 to 4 co-occurrences); cohort activity is flat at 60 to 66% |

| Funnel | 100,000 sessions, 65,082 carts, 29,156 checkouts, 20,436 purchases (20.4%) |

| CLV | Platinum 25,602 vs Bronze 4,145 projected 12-month revenue; sessions vs CLV correlation 0.156; 76 high-engagement non-buyers |



<table>

&#x20; <tr>

&#x20;   <td align="center"><img src="visuals/charts/chart4\_conversion\_funnel.png" alt="Conversion funnel" width="100%"><br><sub>Conversion funnel</sub></td>

&#x20;   <td align="center"><img src="visuals/charts/chart1\_weekly\_revenue.png" alt="Weekly revenue" width="100%"><br><sub>Weekly revenue</sub></td>

&#x20; </tr>

&#x20; <tr>

&#x20;   <td align="center"><img src="visuals/charts/chart2\_clv\_tiers.png" alt="Customer tiers" width="100%"><br><sub>Customer value tiers</sub></td>

&#x20;   <td align="center"><img src="visuals/charts/chart3\_category\_revenue\_vs\_profit.png" alt="Category revenue against profit" width="100%"><br><sub>Category revenue against estimated profit</sub></td>

&#x20; </tr>

</table>



\## Design decisions



\- \*\*MongoDB.\*\* The category (with its average margin) is embedded in each product, the purchase summary in each user, the line items in each transaction, and the page views in each session.

\- \*\*HBase row key.\*\* `user\_id#(9999999999 - epoch\_seconds)` makes a prefix scan return a user's newest sessions first. Column families `info`, `device` and `activity` group columns that are read together.

\- \*\*Spark.\*\* Sessions are converted to JSON Lines so they are read in parallel; explicit schemas (the cart is a map, not 5,000 columns); Parquet output; `shuffle.partitions = 8` for local mode.

\- \*\*Integration.\*\* Extract then join, because 8 GB of RAM cannot run MongoDB, HBase and Spark together.



\## Limitations



\- The generator assigns sessions, purchases, referrers and devices at random, so conversion by channel, cohort decay and engagement against value are not realistic. The pipelines are what is demonstrated.

\- The generator creates no `subcategory\_id` on products, so analysis is at category level.

\- About 90 days of data: the CLV projection extrapolates linearly (no churn or seasonality) and ignores discounts in the profit estimate.

\- Some transactions are not linked to any session, so product purchases can exceed product views.

\- The extracts between systems are snapshots, not live connectors.



\## Evidence and report



\- Screenshots of every step are in \[`screenshots/`](screenshots/).

\- The technical report is in \[`docs/`](docs/).

\- Result files of every analysis are in \[`results/`](results/).



\---



<p align="center"><sub>AUCA Big Data Analytics final project \&middot; Erneste UWIMPUHWE \&middot; 101414</sub></p>

