\# Distributed Multi-Model Analytics for E-commerce Data



AUCA Big Data Analytics final project (individual). \*\*Student:\*\* Erneste UWIMPUHWE, ID 101414.



Uses \*\*MongoDB\*\* (document model), \*\*HBase\*\* (wide-column model) and \*\*Apache Spark\*\* (distributed processing) on a synthetic e-commerce dataset (10,000 users, 5,000 products, 25 categories, 100,000 sessions, 30,000 transactions, about 90 days of activity).



\## Architecture



```

dataset\_generator.py --> JSON files (users, products, categories, transactions, sessions\_\*.json)

&#x20;       |

&#x20;       +--> MongoDB (port 27018)   products (category embedded), users (purchase\_summary embedded),

&#x20;       |                           transactions (items embedded), sessions (page\_views nested)

&#x20;       +--> HBase (Docker)         user\_sessions   row key user\_id#reverse\_timestamp

&#x20;       |                           product\_daily\_metrics   row key product\_id#yyyymmdd

&#x20;       +--> Spark (local\[2])       clean -> Parquet, recommendations, cohorts, Spark SQL

&#x20;                                   CLV job joins a MongoDB extract with an HBase extract

```



| Store | Holds | Why |

|---|---|---|

| MongoDB | catalog, user profiles, transactions, nested session journeys | nested documents and flexible aggregation |

| HBase | per-session event rows, per-product daily metrics | key-ordered time-series scans (newest session first) |

| Spark | cleaned Parquet tables, joins across systems | batch processing, SQL, cross-system joins |



\## Repository layout



| Folder | Content |

|---|---|

| `data\_generation/` | dataset generator (scaled to 100,000 sessions and 30,000 transactions) |

| `mongodb/` | loader and the three aggregation pipelines |

| `hbase/` | table creation, loader, shell and Python queries |

| `spark/` | JSON Lines conversion, cleaning, recommendations and cohorts, Spark SQL, chart data |

| `integration/` | MongoDB and HBase extracts and the CLV job |

| `visuals/` | `make\_charts.py` and the generated `charts/` |

| `results/` | CSV outputs of every analysis |

| `screenshots/` | evidence for each step |



\## How to reproduce (Windows, Python 3.13, Java 17, Spark 4.2.0, Docker Desktop, MongoDB 8.2)



Work in `D:\\BDA\_Project`. Memory is tight on 8 GB, so run one heavy tool at a time.



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



\## Key results



| Area | Result |

|---|---|

| MongoDB top seller | `prod\_01476`, 61 units (also confirmed by Spark) |

| MongoDB revenue by category | top revenue category ranks 15th by estimated profit; the 17th by revenue ranks 1st |

| MongoDB segmentation | 4,962 regular buyers (3 to 5 orders) produce about 62% of total spend |

| HBase | 100,000 session rows; newest-first prefix scan per user; 255,199 product-day metric rows |

| Spark cleaning | 5 tables typed, de-duplicated and validated; sessions flattened into page views and cart items |

| Spark pairs and cohorts | lift is unreliable at very low support (2 to 4 co-occurrences); cohort activity is flat at 60 to 66% |

| Funnel | 100,000 sessions, 65,082 carts, 29,156 checkouts, 20,436 purchases (20.4%); biggest loss at cart to checkout |

| CLV (MongoDB + HBase + Spark) | Platinum 25,602 vs Bronze 4,145 projected 12-month revenue; sessions vs CLV correlation 0.156; 76 high-engagement non-buyers |



Cross-engine checks: category revenue and top-product counts are identical in MongoDB and Spark; the embedded `purchase\_summary` matches the aggregation pipeline exactly (0 mismatches).



\## Design decisions



\- \*\*MongoDB:\*\* category embedded in each product (with its average margin), purchase summary embedded in each user, line items embedded in each transaction, page views nested in each session.

\- \*\*HBase row key:\*\* `user\_id#(9999999999 - epoch\_seconds)` so a prefix scan returns a user's newest sessions first. Column families `info`, `device`, `activity` group columns that are read together.

\- \*\*Spark:\*\* sessions converted to JSON Lines so they can be read in parallel; explicit schemas (the cart is a map, not 5,000 columns); Parquet outputs; `shuffle.partitions = 8` for local mode.

\- \*\*Integration:\*\* extract-then-join, because 8 GB of RAM cannot run MongoDB, HBase and Spark together.



\## Limitations



\- The generator assigns sessions, purchases, referrers and devices at random, so behavioural patterns (conversion by channel, cohort decay, engagement vs value) are not realistic. The pipelines are what is being demonstrated.

\- The generator does not create `subcategory\_id` on products; analysis is at category level.

\- About 90 days of data: the CLV projection extrapolates linearly (no churn or seasonality); discounts are ignored in the profit estimate.

\- Some transactions are not linked to any session, so product purchases can exceed product views.

\- Extracts between systems are snapshots, not live connectors.

