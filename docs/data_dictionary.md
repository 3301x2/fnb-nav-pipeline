# Data Dictionary

## Source data (raw)

### customer_spend.base_data
One row per customer per month. 82M+ rows.

| Raw Field | Renamed To | Type | Description |
|-----------|-----------|------|-------------|
| UNIQUE_ID | UNIQUE_ID | STRING | Unique customer identifier (hashed) |
| demo_1 | gender | INTEGER | 0 = Male, 1 = Female |
| demo_2 | age | FLOAT | Customer age in years (18-100) |
| demo_3 | profile_age | FLOAT | Age of customer profile in years |
| demo_4 | vertical_sales_index | INTEGER | Current Vertical Sales Index |
| demo_5 | income_segment | STRING | Income-based segment (EW0, EB0, GLD, etc.) |
| demo_6 | hyper_segment | STRING | FNB hyper-segmentation (Salaried, Self-employed, etc.) |
| demo_7 | estimated_income | FLOAT | Smoothed credit turnover income estimate |
| demo_8 | main_banked | INTEGER | 1 = FNB main-banked customer |
| demo_9 | credit_risk_class | STRING | PRISM credit risk category |

### customer_spend.transaction_data
One row per transaction. 2.2B+ rows in production.

| Field | Type | Description |
|-------|------|-------------|
| UNIQUE_ID | STRING | Customer identifier |
| EFF_DATE | DATE | Transaction date |
| CATEGORY_ONE_ID | INTEGER | Top-level category (→ spend_lookups.category_one_id) |
| CATEGORY_TWO_ID | INTEGER | Sub-category (→ spend_lookups.category_two_id) |
| NAV_CATEGORY_ID | INTEGER | NAV category (→ spend_lookups.nav_category_id) |
| DESTINATION_ID | INTEGER | Merchant/shop (→ spend_lookups.destination_id) |
| LOCATION_ID | FLOAT | Physical location (→ spend_lookups.location_id) |
| SUBURB_ID | FLOAT | Suburb (→ spend_lookups.suburb_id) |
| trns_time | STRING | Transaction time (HHMM format) |
| trns_amt | FLOAT | Transaction amount in Rands |

---

## Pipeline tables

### staging.stg_transactions
All lookups joined, PII stripped. Partitioned by month, clustered by CATEGORY_TWO + DESTINATION.

| Column | Type | Source |
|--------|------|--------|
| UNIQUE_ID | STRING | transaction_data |
| EFF_DATE | DATE | transaction_data |
| trns_amt | FLOAT | transaction_data |
| trns_time | STRING | transaction_data |
| trns_year, trns_month, trns_dow, trns_hour | INT | Extracted from EFF_DATE / trns_time |
| CATEGORY_ONE, CATEGORY_TWO, NAV_CATEGORY | STRING | Lookup joins |
| DESTINATION | STRING | destination_id lookup |
| PROVINCE, MUNICIPALITY, SUBURB, TOWN | STRING | suburb_id lookup |
| LOCATION_NAME, latitude, longitude | STRING/FLOAT | location_id lookup |

### staging.stg_customers
One row per customer. Demographics renamed.

| Column | Type | Description |
|--------|------|-------------|
| UNIQUE_ID | STRING | Customer identifier |
| gender | INTEGER | 0=Male, 1=Female |
| age | FLOAT | Years |
| estimated_income | FLOAT | Smoothed credit turnover |
| income_segment | STRING | EW0, EB0, GLD, etc. |
| age_group | STRING | Pre-binned: 18-25, 26-35, 36-45, 46-60, 60+ |
| income_group | STRING | Pre-binned: R0-R5.5k through R56k+ |
| gender_label | STRING | Male / Female / Unknown |

### analytics.int_rfm_features
20+ behavioral features per customer (last 12 months).

| Column | Type | Description |
|--------|------|-------------|
| nr_trns | INTEGER | Number of transactions |
| val_trns | FLOAT | Total spend |
| avg_val | FLOAT | Average transaction value |
| median_val | FLOAT | Median transaction value |
| std_val | FLOAT | Std deviation of transaction values |
| lst_trns_days | INTEGER | Days since last transaction |
| active_months | INTEGER | Months with at least 1 transaction |
| mnthly_avg_nr | FLOAT | Average transactions per month |
| mnthly_avg_val | FLOAT | Average spend per month |
| days_between | FLOAT | Average days between transactions |
| avg_dow | FLOAT | Average day of week |
| avg_hour | FLOAT | Average hour of day |
| NR_TRNS_MORNING | INTEGER | Transactions 6am-10am |
| NR_TRNS_MIDDAY | INTEGER | Transactions 11am-4pm |
| NR_TRNS_EVENING | INTEGER | Transactions 5pm-9pm |
| NR_TRNS_LATE | INTEGER | Transactions 10pm-5am |
| NR_TRNS_WEEKEND | INTEGER | Weekend transactions |
| NR_TRNS_WEEK | INTEGER | Weekday transactions |
| active_nav_categories | INTEGER | Distinct categories |
| active_destinations | INTEGER | Distinct merchants |
| active_suburbs | INTEGER | Distinct suburbs |
| active_locations | INTEGER | Distinct locations |

### analytics.int_customer_category_spend
Per customer × category × destination. Powers share-of-wallet.

| Column | Type | Description |
|--------|------|-------------|
| UNIQUE_ID | STRING | Customer |
| CATEGORY_TWO | STRING | Category name |
| DESTINATION | STRING | Merchant name |
| dest_spend | FLOAT | Spend at this destination |
| category_spend | FLOAT | Total category spend |
| share_of_wallet_pct | FLOAT | dest_spend / category_spend × 100 |

### marts.mart_cluster_output
Every customer with cluster assignment + demographics.

| Column | Type | Description |
|--------|------|-------------|
| cluster_id | INTEGER | 1-5 (ranked by spend) |
| segment_name | STRING | Champions, Loyal High Value, Steady Mid-Tier, At Risk, Dormant |
| r_score, f_score, m_score | INTEGER | RFM quintile scores (1-5) |
| All int_rfm_features columns | Various | Full feature set |
| All stg_customers columns | Various | Full demographics |

### marts.mart_destination_benchmarks
Every destination's KPIs within its category. Dashboard anonymizes competitors.

| Column | Type | Description |
|--------|------|-------------|
| CATEGORY_TWO | STRING | Category |
| DESTINATION | STRING | Real merchant name (dashboard anonymizes) |
| market_share_pct | FLOAT | % of category spend |
| penetration_pct | FLOAT | % of category customers |
| spend_rank | INTEGER | Rank within category |
