"""
FNB NAV - Data Insights Dashboard
Client selection happens here not in SQL.
Pick a category, pick a client, everything filters live.
Competitors auto-anonymize.

Run: streamlit run dashboards/app.py
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go

# -- config --

PROJECT = "fmn-sandbox"
client = bigquery.Client(project=PROJECT)

COLORS = ["#2E75B6", "#4CAF50", "#FF9800", "#E91E63", "#9C27B0",
          "#00BCD4", "#795548", "#607D8B"]

RISK_COLORS = {
    "Churned": "#d32f2f", "High Risk": "#f57c00",
    "Medium Risk": "#fbc02d", "Low Risk": "#4caf50", "Stable": "#2196f3"
}

@st.cache_data(ttl=300)
def q(query: str) -> pd.DataFrame:
    return client.query(query).to_dataframe()


def wip_banner(feature_name):
    st.info(
        f"🚧 **Work in progress** — *{feature_name}* is an early preview. "
        f"We'd love your feedback on what would make this more valuable."
    )


def format_rand(val):
    """Format a number as Rands."""
    if val >= 1_000_000:
        return f"R{val/1_000_000:,.1f}M"
    elif val >= 1_000:
        return f"R{val/1_000:,.1f}k"
    return f"R{val:,.0f}"


# -- layout + sidebar --

st.set_page_config(page_title="FNB NAV — Data Insights", page_icon="📊", layout="wide")
st.markdown("""<style>
    .block-container { padding-top: 1rem; }
    [data-testid="stMetricValue"] { font-size: 1.5rem; }
</style>""", unsafe_allow_html=True)

st.sidebar.title("NAV Data Platform")
st.sidebar.markdown(f"**Project:** `{PROJECT}`")
st.sidebar.markdown("---")

# -- client selection (the core feature) --
st.sidebar.subheader("Client Configuration")

try:
    categories = q(f"""
        SELECT DISTINCT CATEGORY_TWO
        FROM `{PROJECT}.marts.mart_destination_benchmarks`
        WHERE CATEGORY_TWO IS NOT NULL
        ORDER BY CATEGORY_TWO
    """)
    category_list = categories["CATEGORY_TWO"].tolist()
except Exception as e:
    category_list = []
    st.sidebar.error(f"Category query failed: {e}")

selected_category = st.sidebar.selectbox(
    "Category", category_list,
    index=category_list.index("Clothing & Apparel") if "Clothing & Apparel" in category_list else 0
)

try:
    destinations = q(f"""
        SELECT DESTINATION
        FROM `{PROJECT}.marts.mart_destination_benchmarks`
        WHERE CATEGORY_TWO = '{selected_category}'
        ORDER BY total_spend DESC
        LIMIT 50
    """)
    dest_list = destinations["DESTINATION"].tolist()
except Exception as e:
    dest_list = []
    st.sidebar.error(f"Destination query failed: {e}")

selected_client = st.sidebar.selectbox(
    "Client (pitch target)", dest_list,
    index=dest_list.index("Adidas") if "Adidas" in dest_list else 0
)

top_n_competitors = st.sidebar.slider("Competitors to show", 3, 10, 5)

st.sidebar.markdown("---")

page = st.sidebar.radio("Navigate", [
    "📊 Executive Summary",
    "👥 Customer Segments",
    "💰 Spend Share",
    "🧬 Demographics",
    "📈 Trends",
    "🕐 Behavioral",
    "🗺️ Geo Insights",
    "⚠️ Churn Risk",
    "📊 Benchmarks",
    "💡 ROI Simulator",
    "🤖 ML Evaluation",
    "🏥 Data Health"
])


# -- page 1: executive summary --

if page == "📊 Executive Summary":
    st.title("Executive Summary")
    st.caption(f"Category: **{selected_category}** · Client: **{selected_client}**")

    try:
        # Pipeline row counts
        counts_df = q(f"""
            SELECT 'stg_transactions' AS tbl, COUNT(*) AS n FROM `{PROJECT}.staging.stg_transactions`
            UNION ALL SELECT 'stg_customers', COUNT(*) FROM `{PROJECT}.staging.stg_customers`
            UNION ALL SELECT 'rfm_features', COUNT(*) FROM `{PROJECT}.analytics.int_rfm_features`
            UNION ALL SELECT 'cluster_output', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_output`
        """)
        counts = dict(zip(counts_df["tbl"], counts_df["n"]))

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total transactions", f"{counts.get('stg_transactions', 0):,}")
        c2.metric("Total customers", f"{counts.get('stg_customers', 0):,}")
        c3.metric("Segmented customers", f"{counts.get('rfm_features', 0):,}")
        c4.metric("Cluster assignments", f"{counts.get('cluster_output', 0):,}")

        # Client headline KPIs
        client_kpi = q(f"""
            SELECT
                customers, total_spend, market_share_pct, penetration_pct, avg_txn_value
            FROM `{PROJECT}.marts.mart_destination_benchmarks`
            WHERE CATEGORY_TWO = '{selected_category}'
              AND DESTINATION = '{selected_client}'
        """)

        if not client_kpi.empty:
            r = client_kpi.iloc[0]
            st.subheader(f"{selected_client} in {selected_category}")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Customers", f"{int(r['customers']):,}")
            k2.metric("Total spend", format_rand(r["total_spend"]))
            k3.metric("Market share", f"{r['market_share_pct']}%")
            k4.metric("Penetration", f"{r['penetration_pct']}%")

        # Segment overview
        summary = q(f"""
            SELECT segment_name, customer_count, pct_of_total,
                   avg_total_spend, recommended_action
            FROM `{PROJECT}.marts.mart_cluster_summary`
            ORDER BY avg_total_spend DESC
        """)
        if not summary.empty:
            st.subheader("Customer segment overview")
            st.dataframe(summary.rename(columns={
                "segment_name": "Segment", "customer_count": "Customers",
                "pct_of_total": "% of total", "avg_total_spend": "Avg spend (R)",
                "recommended_action": "Action"
            }), use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 2: customer segments --

elif page == "👥 Customer Segments":
    st.title("Customer Segments")
    st.markdown("""
    K-means clustering groups customers into 5 segments based on spending behavior,
    frequency, and recency. The algorithm finds natural groupings — no manual rules.
    """)

    try:
        profiles = q(f"SELECT * FROM `{PROJECT}.marts.mart_cluster_profiles` ORDER BY avg_total_spend DESC")
        summary = q(f"SELECT * FROM `{PROJECT}.marts.mart_cluster_summary` ORDER BY avg_total_spend DESC")

        if not profiles.empty:
            total = profiles["customer_count"].sum()
            c1, c2, c3 = st.columns(3)
            c1.metric("Total segmented", f"{int(total):,}")
            c2.metric("Segments", len(profiles))
            c3.metric("Top segment", profiles.iloc[0]["segment_name"])

            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(profiles, values="customer_count", names="segment_name",
                             title="Customer distribution", color_discrete_sequence=COLORS)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.scatter(profiles, x="avg_transactions", y="avg_total_spend",
                                 size="customer_count", color="segment_name",
                                 title="Spend vs frequency",
                                 labels={"avg_transactions": "Avg transactions",
                                         "avg_total_spend": "Avg spend (R)"},
                                 color_discrete_sequence=COLORS)
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("Segment profiles")
            for _, row in summary.iterrows():
                seg = row["segment_name"]
                prof = profiles[profiles["segment_name"] == seg]
                if prof.empty:
                    continue
                p = prof.iloc[0]

                with st.expander(f"**{seg}** — {int(p['customer_count']):,} customers ({p['pct_of_total']}%)"):
                    mc1, mc2, mc3, mc4 = st.columns(4)
                    mc1.metric("Avg spend", format_rand(p["avg_total_spend"]))
                    mc2.metric("Avg txns", f"{p['avg_transactions']:.0f}")
                    mc3.metric("Avg recency", f"{p['avg_recency_days']:.0f} days")
                    mc4.metric("Avg txn value", format_rand(p["avg_txn_value"]))

                    st.markdown(f"*{row['business_description']}*")
                    st.success(f"**Action:** {row['recommended_action']}")

                    # Age distribution
                    age_cols = ["age_18_25", "age_26_35", "age_36_45", "age_46_60", "age_over_60"]
                    if all(c in p.index for c in age_cols):
                        age_df = pd.DataFrame({
                            "Age": ["18-25", "26-35", "36-45", "46-60", "60+"],
                            "Count": [p[c] for c in age_cols]
                        })
                        fig = px.bar(age_df, x="Age", y="Count",
                                     title=f"Age distribution — {seg}",
                                     color_discrete_sequence=[COLORS[0]])
                        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 3: spend share --

elif page == "💰 Spend Share":
    st.title("Spend Share Analysis")
    st.caption(f"Category: **{selected_category}** · Client: **{selected_client}**")
    st.markdown(f"""
    For each customer in **{selected_category}**, what percentage of their spend goes
    to **{selected_client}**? If someone spends R1,000 on the category and R350 at
    {selected_client}, their share of wallet is 35%.
    """)

    try:
        agg = q(f"""
            WITH cat_customers AS (
                SELECT UNIQUE_ID, SUM(dest_spend) AS category_spend
                FROM `{PROJECT}.analytics.int_customer_category_spend`
                WHERE CATEGORY_TWO = '{selected_category}'
                GROUP BY UNIQUE_ID
            ),
            client_customers AS (
                SELECT UNIQUE_ID, SUM(dest_spend) AS client_spend, AVG(share_of_wallet_pct) AS sow
                FROM `{PROJECT}.analytics.int_customer_category_spend`
                WHERE CATEGORY_TWO = '{selected_category}' AND DESTINATION = '{selected_client}'
                GROUP BY UNIQUE_ID
            )
            SELECT
                COUNT(DISTINCT cc.UNIQUE_ID) AS category_customers,
                COUNTIF(cl.client_spend > 0) AS client_customers,
                ROUND(COUNTIF(cl.client_spend > 0) * 100.0 / COUNT(DISTINCT cc.UNIQUE_ID), 1) AS penetration,
                ROUND(AVG(CASE WHEN cl.client_spend > 0 THEN cl.sow END), 1) AS avg_share,
                ROUND(SUM(cl.client_spend), 0) AS total_client_spend,
                ROUND(SUM(cc.category_spend), 0) AS total_category_spend
            FROM cat_customers cc
            LEFT JOIN client_customers cl ON cc.UNIQUE_ID = cl.UNIQUE_ID
        """)

        if not agg.empty:
            r = agg.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Category customers", f"{int(r['category_customers']):,}")
            c2.metric("Client customers", f"{int(r['client_customers']):,}")
            c3.metric("Penetration", f"{r['penetration']}%")
            c4.metric("Avg share of wallet", f"{r['avg_share']}%")

        # Wallet band distribution
        bands = q(f"""
            SELECT
                CASE
                    WHEN share_of_wallet_pct >= 80 THEN '80-100% (Loyalist)'
                    WHEN share_of_wallet_pct >= 50 THEN '50-80% (Primary)'
                    WHEN share_of_wallet_pct >= 20 THEN '20-50% (Secondary)'
                    ELSE '1-20% (Occasional)'
                END AS wallet_band,
                COUNT(DISTINCT UNIQUE_ID) AS customers,
                ROUND(SUM(dest_spend), 0) AS client_spend
            FROM `{PROJECT}.analytics.int_customer_category_spend`
            WHERE CATEGORY_TWO = '{selected_category}'
              AND DESTINATION = '{selected_client}'
            GROUP BY wallet_band ORDER BY wallet_band
        """)

        if not bands.empty:
            fig = px.bar(bands, x="wallet_band", y="customers",
                         color="client_spend", color_continuous_scale="Greens",
                         title="Share of wallet distribution",
                         labels={"wallet_band": "Wallet band", "customers": "Customers"})
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 4: demographics --

elif page == "🧬 Demographics":
    st.title("Demographic Breakdown")
    st.caption(f"Category: **{selected_category}**")

    try:
        demo = q(f"""
            SELECT * FROM `{PROJECT}.marts.mart_demographic_summary`
            WHERE CATEGORY_TWO = '{selected_category}'
        """)

        if not demo.empty:
            col1, col2 = st.columns(2)
            with col1:
                age_agg = demo.groupby("age_group").agg(
                    customers=("customers", "sum"),
                    spend=("total_spend", "sum")
                ).reset_index()
                fig = px.bar(age_agg, x="age_group", y="customers",
                             color="spend", color_continuous_scale="Blues",
                             title="Customers by age group")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                gender_agg = demo.groupby("gender_label").agg(
                    customers=("customers", "sum")
                ).reset_index()
                fig = px.pie(gender_agg, values="customers", names="gender_label",
                             title="By gender", color_discrete_sequence=COLORS)
                st.plotly_chart(fig, use_container_width=True)

            # Income breakdown
            income_agg = demo.groupby("income_group").agg(
                customers=("customers", "sum"),
                spend=("total_spend", "sum")
            ).reset_index().sort_values("spend", ascending=False)
            fig = px.bar(income_agg, x="income_group", y="spend",
                         color="customers", color_continuous_scale="Oranges",
                         title="Spend by income group")
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Detailed breakdown")
            st.dataframe(demo, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 5: trends --

elif page == "📈 Trends":
    st.title("Spend Trends")
    st.caption(f"Category: **{selected_category}** · Client: **{selected_client}**")
    st.markdown("""
    Monthly trend: client spend vs total category. The gap = competitor spend (anonymous).
    """)

    try:
        # Category totals per month
        cat_trends = q(f"""
            SELECT month,
                   SUM(txn_count) AS category_txns,
                   SUM(customer_count) AS category_customers,
                   SUM(total_spend) AS category_spend
            FROM `{PROJECT}.marts.mart_monthly_trends`
            WHERE CATEGORY_TWO = '{selected_category}'
            GROUP BY month ORDER BY month
        """)

        # Client per month
        client_trends = q(f"""
            SELECT month,
                   SUM(txn_count) AS client_txns,
                   SUM(customer_count) AS client_customers,
                   SUM(total_spend) AS client_spend
            FROM `{PROJECT}.marts.mart_monthly_trends`
            WHERE CATEGORY_TWO = '{selected_category}'
              AND DESTINATION = '{selected_client}'
            GROUP BY month ORDER BY month
        """)

        if not cat_trends.empty:
            trends = cat_trends.merge(client_trends, on="month", how="left").fillna(0)
            trends["client_share_pct"] = round(
                trends["client_spend"] / trends["category_spend"].replace(0, pd.NA) * 100, 1
            )

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trends["month"], y=trends["category_spend"],
                mode="lines+markers", name="Category total",
                line=dict(color=COLORS[7], width=2, dash="dash")))
            fig.add_trace(go.Scatter(
                x=trends["month"], y=trends["client_spend"],
                mode="lines+markers", name=f"{selected_client}",
                line=dict(color=COLORS[0], width=3)))
            fig.update_layout(title=f"Monthly spend: {selected_client} vs {selected_category}",
                              xaxis_title="Month", yaxis_title="Spend (R)", height=400)
            st.plotly_chart(fig, use_container_width=True)

            fig2 = px.line(trends, x="month", y="client_share_pct",
                           title=f"{selected_client} share of {selected_category} (%)",
                           color_discrete_sequence=[COLORS[3]])
            fig2.update_layout(height=300)
            st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 6: behavioral --

elif page == "🕐 Behavioral":
    st.title("Behavioral Insights")
    st.markdown("""
    Each segment has different shopping patterns — when they shop, how many categories
    they explore, weekend vs weekday. This helps personalize campaigns.
    """)

    try:
        behav = q(f"""
            SELECT * FROM `{PROJECT}.marts.mart_behavioral_summary`
            ORDER BY avg_txns_per_customer DESC
        """)

        if not behav.empty:
            st.subheader("When do they shop?")
            time_df = behav[["segment_name", "pct_morning", "pct_afternoon",
                             "pct_evening", "pct_late_night"]].melt(
                id_vars="segment_name", var_name="time_slot", value_name="pct"
            )
            time_df["time_slot"] = (time_df["time_slot"]
                                    .str.replace("pct_", "")
                                    .str.replace("_", " ")
                                    .str.title())

            fig = px.bar(time_df, x="segment_name", y="pct", color="time_slot",
                         title="Transaction distribution by time of day",
                         labels={"pct": "% of transactions", "segment_name": ""},
                         color_discrete_sequence=COLORS, barmode="stack")
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Weekend vs weekday")
            fig2 = px.bar(behav, x="segment_name", y="pct_weekend",
                          title="Weekend transaction %",
                          color_discrete_sequence=[COLORS[2]])
            st.plotly_chart(fig2, use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                fig3 = px.bar(behav, x="segment_name", y="avg_categories",
                              title="Avg categories shopped",
                              color_discrete_sequence=[COLORS[4]])
                st.plotly_chart(fig3, use_container_width=True)
            with col2:
                fig4 = px.bar(behav, x="segment_name", y="avg_merchants",
                              title="Avg merchants visited",
                              color_discrete_sequence=[COLORS[5]])
                st.plotly_chart(fig4, use_container_width=True)

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 7: geo --

elif page == "🗺️ Geo Insights":
    st.title("Geographic Insights")
    st.caption(f"Category: **{selected_category}**")

    try:
        prov = q(f"""
            SELECT PROVINCE,
                   SUM(customers) AS customers,
                   SUM(transactions) AS transactions,
                   SUM(total_spend) AS total_spend,
                   ROUND(SUM(total_spend) * 100.0 / SUM(SUM(total_spend)) OVER(), 1) AS pct
            FROM `{PROJECT}.marts.mart_geo_summary`
            WHERE CATEGORY_TWO = '{selected_category}'
            GROUP BY PROVINCE ORDER BY total_spend DESC
        """)

        if not prov.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(prov, x="total_spend", y="PROVINCE", orientation="h",
                             color="customers", color_continuous_scale="Blues",
                             title="Spend by province",
                             labels={"total_spend": "Spend (R)", "PROVINCE": ""})
                fig.update_layout(yaxis=dict(autorange="reversed"), height=400)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig2 = px.pie(prov, values="total_spend", names="PROVINCE",
                              title="Province share", color_discrete_sequence=COLORS)
                st.plotly_chart(fig2, use_container_width=True)

        # Top municipalities
        munic = q(f"""
            SELECT PROVINCE, MUNICIPALITY, SUM(customers) AS customers, SUM(total_spend) AS total_spend
            FROM `{PROJECT}.marts.mart_geo_summary`
            WHERE CATEGORY_TWO = '{selected_category}'
            GROUP BY PROVINCE, MUNICIPALITY
            ORDER BY total_spend DESC LIMIT 15
        """)
        if not munic.empty:
            st.subheader("Top 15 municipalities")
            fig3 = px.bar(munic, x="total_spend", y="MUNICIPALITY", orientation="h",
                          color="PROVINCE", title="Top municipalities by spend",
                          color_discrete_sequence=COLORS)
            fig3.update_layout(yaxis=dict(autorange="reversed"), height=500)
            st.plotly_chart(fig3, use_container_width=True)

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 8: churn risk --

elif page == "⚠️ Churn Risk":
    st.title("Churn Risk Analysis")
    st.markdown("""
    **ML-powered churn prediction** using a logistic regression classifier trained on
    customer behavioral patterns. Each customer gets a churn probability score (0-100%)
    based on 15 features including transaction trends, shopping diversity, and demographics.
    """)

    try:
        risk = q(f"""
            SELECT churn_risk_level,
                   COUNT(*) AS customers,
                   ROUND(AVG(total_spend), 0) AS avg_spend,
                   ROUND(AVG(churn_probability) * 100, 1) AS avg_churn_pct,
                   ROUND(AVG(days_since_last), 0) AS avg_days_since_last,
                   ROUND(AVG(txns_last_3m), 1) AS avg_recent_txns,
                   ROUND(SUM(total_spend), 0) AS total_spend_at_risk
            FROM `{PROJECT}.marts.mart_churn_risk`
            GROUP BY churn_risk_level
            ORDER BY CASE churn_risk_level
                WHEN 'Critical' THEN 1 WHEN 'High' THEN 2
                WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 ELSE 5 END
        """)

        if not risk.empty:
            total_cust = risk["customers"].sum()

            # KPI row
            critical = risk[risk["churn_risk_level"] == "Critical"]
            high = risk[risk["churn_risk_level"] == "High"]
            at_risk_cust = 0
            at_risk_spend = 0
            if not critical.empty:
                at_risk_cust += critical.iloc[0]["customers"]
                at_risk_spend += critical.iloc[0]["total_spend_at_risk"]
            if not high.empty:
                at_risk_cust += high.iloc[0]["customers"]
                at_risk_spend += high.iloc[0]["total_spend_at_risk"]

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total scored", f"{total_cust:,}")
            k2.metric("Critical + High", f"{int(at_risk_cust):,}")
            k3.metric("Spend at risk", format_rand(at_risk_spend))
            k4.metric("Recovery (10%)", format_rand(at_risk_spend * 0.1))

            churn_colors = {
                "Critical": "#d32f2f", "High": "#f57c00",
                "Medium": "#fbc02d", "Low": "#4caf50", "Stable": "#2196f3"
            }

            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(risk, values="customers", names="churn_risk_level",
                             title="Churn risk distribution (ML-scored)",
                             color="churn_risk_level", color_discrete_map=churn_colors)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig2 = px.bar(risk, x="churn_risk_level", y="total_spend_at_risk",
                              color="churn_risk_level", color_discrete_map=churn_colors,
                              title="Spend at risk by level")
                fig2.update_layout(showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

            # Average churn probability per level
            fig3 = px.bar(risk, x="churn_risk_level", y="avg_churn_pct",
                          color="churn_risk_level", color_discrete_map=churn_colors,
                          title="Average ML churn probability (%)",
                          labels={"avg_churn_pct": "Avg probability %", "churn_risk_level": ""})
            fig3.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig3, use_container_width=True)

            # Detail table
            st.subheader("Risk level breakdown")
            display = risk.copy()
            display["pct_of_total"] = (display["customers"] / total_cust * 100).round(1)
            st.dataframe(display.rename(columns={
                "churn_risk_level": "Risk level", "customers": "Customers",
                "pct_of_total": "% of total", "avg_churn_pct": "Avg churn %",
                "avg_spend": "Avg spend (R)", "avg_days_since_last": "Days since last",
                "avg_recent_txns": "Avg txns (3M)", "total_spend_at_risk": "Spend at risk (R)"
            }), use_container_width=True, hide_index=True)

            if at_risk_cust > 0:
                st.warning(
                    f"**{int(at_risk_cust):,} customers ({at_risk_cust/total_cust*100:.1f}%) are Critical or High risk, "
                    f"representing {format_rand(at_risk_spend)} in historical spend.** "
                    f"A 10% re-engagement rate would recover ~{format_rand(at_risk_spend * 0.1)}."
                )

            # Model performance
            st.subheader("Model performance")
            try:
                model_eval = q(f"""
                    SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.churn_classifier`)
                """)
                if not model_eval.empty:
                    me = model_eval.iloc[0]
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Accuracy", f"{me.get('accuracy', 0):.3f}")
                    m2.metric("Precision", f"{me.get('precision', 0):.3f}")
                    m3.metric("Recall", f"{me.get('recall', 0):.3f}")
                    m4.metric("F1 Score", f"{me.get('f1_score', 0):.3f}")
            except Exception:
                st.info("Model metrics not available. Run train_churn_model.sql first.")

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 9: benchmarks (auto-anonymization) --

elif page == "📊 Benchmarks":
    st.title("Competitive Benchmarks")
    st.caption(f"Category: **{selected_category}** · Client: **{selected_client}**")
    st.markdown(f"""
    **{selected_client}** vs the top {top_n_competitors} competitors in
    **{selected_category}**. Competitor names are anonymized automatically.
    """)

    try:
        bench = q(f"""
            SELECT * FROM `{PROJECT}.marts.mart_destination_benchmarks`
            WHERE CATEGORY_TWO = '{selected_category}'
            ORDER BY total_spend DESC
        """)

        if not bench.empty:
            # auto-anonymization logic
            client_row = bench[bench["DESTINATION"] == selected_client].copy()
            competitors = bench[bench["DESTINATION"] != selected_client].head(top_n_competitors).copy()

            client_row["display_name"] = selected_client
            client_row["entity_type"] = "Client"
            competitors["display_name"] = [
                f"Competitor {i+1}" for i in range(len(competitors))
            ]
            competitors["entity_type"] = "Competitor"

            display = pd.concat([client_row, competitors]).sort_values(
                "total_spend", ascending=False
            )

            if not client_row.empty:
                cr = client_row.iloc[0]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric(f"{selected_client} market share", f"{cr['market_share_pct']}%")
                c2.metric("Penetration", f"{cr['penetration_pct']}%")
                c3.metric("Avg transaction", format_rand(cr["avg_txn_value"]))
                c4.metric("Spend/customer", format_rand(cr["spend_per_customer"]))

            st.subheader(f"{selected_client} vs top {top_n_competitors} competitors")
            color_map = {"Client": COLORS[0], "Competitor": COLORS[7]}

            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(display, x="display_name", y="market_share_pct",
                             color="entity_type", color_discrete_map=color_map,
                             title="Market share (%)")
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig2 = px.bar(display, x="display_name", y="spend_per_customer",
                              color="entity_type", color_discrete_map=color_map,
                              title="Spend per customer (R)")
                fig2.update_layout(showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

            col3, col4 = st.columns(2)
            with col3:
                fig3 = px.bar(display, x="display_name", y="penetration_pct",
                              color="entity_type", color_discrete_map=color_map,
                              title="Customer penetration (%)")
                fig3.update_layout(showlegend=False)
                st.plotly_chart(fig3, use_container_width=True)
            with col4:
                fig4 = px.bar(display, x="display_name", y="avg_txn_value",
                              color="entity_type", color_discrete_map=color_map,
                              title="Avg transaction value (R)")
                fig4.update_layout(showlegend=False)
                st.plotly_chart(fig4, use_container_width=True)

            # Insight
            if not client_row.empty and not competitors.empty:
                avg_comp = competitors["market_share_pct"].mean()
                cr_share = client_row.iloc[0]["market_share_pct"]
                if cr_share > avg_comp:
                    st.success(
                        f"**{selected_client} leads** with {cr_share}% market share "
                        f"vs competitor average of {avg_comp:.1f}%."
                    )
                else:
                    st.warning(
                        f"**{selected_client} trails** at {cr_share}% market share "
                        f"vs competitor average of {avg_comp:.1f}%. Opportunity to grow."
                    )

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 10: roi simulator --

elif page == "💡 ROI Simulator":
    st.title("ROI Simulator")
    wip_banner("ROI Scenario Modelling")
    st.caption(f"Category: **{selected_category}** · Client: **{selected_client}**")

    try:
        baseline = q(f"""
            SELECT
                COUNT(DISTINCT UNIQUE_ID) AS total_customers,
                ROUND(SUM(dest_spend), 0) AS total_revenue,
                ROUND(AVG(dest_spend), 0) AS revenue_per_customer
            FROM `{PROJECT}.analytics.int_customer_category_spend`
            WHERE CATEGORY_TWO = '{selected_category}'
              AND DESTINATION = '{selected_client}'
        """)

        if not baseline.empty:
            b = baseline.iloc[0].fillna(0)
            st.subheader(f"Current baseline — {selected_client}")
            bc1, bc2, bc3 = st.columns(3)
            bc1.metric("Client customers", f"{int(b['total_customers']):,}")
            bc2.metric("Client revenue", format_rand(b["total_revenue"]))
            bc3.metric("Revenue/customer", format_rand(b["revenue_per_customer"]))

            st.markdown("---")
            st.subheader("Scenario modelling")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Increase penetration**")
                pen = st.slider("New customers (%)", 1, 50, 10)
                new_cust = int(b["total_customers"] * pen / 100)
                new_rev = new_cust * b["revenue_per_customer"]
                st.metric("Additional customers", f"{new_cust:,}")
                st.metric("Additional revenue", format_rand(new_rev))
            with col2:
                st.markdown("**Increase spend per customer**")
                spd = st.slider("Spend increase (%)", 1, 50, 10)
                add_spend = b["total_revenue"] * spd / 100
                st.metric("Additional revenue", format_rand(add_spend))

            st.markdown("---")
            combined = new_rev + add_spend
            cost = st.number_input("Campaign cost (R)", value=500000, step=50000)
            roi = (combined - cost) / cost * 100 if cost > 0 else 0

            rc1, rc2, rc3 = st.columns(3)
            rc1.metric("Projected revenue", format_rand(combined))
            rc2.metric("Campaign cost", format_rand(cost))
            rc3.metric("ROI", f"{roi:,.0f}%", delta=format_rand(combined - cost))

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 11: ml evaluation --

elif page == "🤖 ML Evaluation":
    st.title("ML Model Evaluation")
    st.markdown("""
    **K-means clustering model** trained on 9 customer features using BigQuery ML.
    This page shows whether the model found meaningful, well-separated segments.
    """)

    try:
        # Model metrics
        st.subheader("Model metrics")
        eval_df = q(f"""
            SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.kmeans_customer_segments`)
        """)

        if not eval_df.empty:
            e = eval_df.iloc[0]
            c1, c2 = st.columns(2)
            c1.metric("Davies-Bouldin index", f"{e['davies_bouldin_index']:.4f}",
                       help="Lower = better separated clusters. Under 2.0 is good for business use.")
            c2.metric("Mean squared distance", f"{e['mean_squared_distance']:.4f}",
                       help="Average distance from each customer to their cluster center. Lower = tighter clusters.")

            if e['davies_bouldin_index'] < 2.0:
                st.success(f"Davies-Bouldin index of {e['davies_bouldin_index']:.4f} indicates well-separated clusters.")
            else:
                st.warning(f"Davies-Bouldin index of {e['davies_bouldin_index']:.4f} is above 2.0 — consider tuning k or features.")

        # Training convergence
        st.subheader("Training convergence")
        st.markdown("Loss should decrease and then flatten — that means the model found stable cluster centers.")
        training = q(f"""
            SELECT iteration,
                   ROUND(loss, 4) AS loss,
                   ROUND(loss - LAG(loss) OVER (ORDER BY iteration), 4) AS improvement
            FROM ML.TRAINING_INFO(MODEL `{PROJECT}.analytics.kmeans_customer_segments`)
            ORDER BY iteration
        """)

        if not training.empty:
            fig = px.line(training, x="iteration", y="loss",
                          title="Training loss per iteration",
                          markers=True, color_discrete_sequence=[COLORS[0]])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

            last_improvement = training["improvement"].iloc[-1]
            if last_improvement is not None and abs(last_improvement) < 0.01:
                st.success("Model converged — loss is stable in the final iterations.")
            else:
                st.info("Check if loss is still decreasing. If so, consider increasing max_iterations.")

        # Cluster centroids
        st.subheader("Cluster centroids")
        st.markdown("The center of each cluster — the average feature values that **define** each segment.")
        centroids = q(f"""
            SELECT centroid_id, feature, ROUND(numerical_value, 2) AS value
            FROM ML.CENTROIDS(MODEL `{PROJECT}.analytics.kmeans_customer_segments`)
            ORDER BY centroid_id, feature
        """)

        if not centroids.empty:
            pivot = centroids.pivot(index="feature", columns="centroid_id", values="value")
            st.dataframe(pivot, use_container_width=True)

        # Cluster sizes and balance
        st.subheader("Cluster balance")
        st.markdown("Ideally no single cluster has more than 40% of customers.")
        sizes = q(f"""
            SELECT segment_name, COUNT(*) AS customers,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
            FROM `{PROJECT}.marts.mart_cluster_output`
            GROUP BY segment_name
            ORDER BY customers DESC
        """)

        if not sizes.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(sizes, values="customers", names="segment_name",
                             title="Cluster distribution",
                             color_discrete_sequence=COLORS)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig2 = px.bar(sizes, x="segment_name", y="pct",
                              title="% of customers per segment",
                              color_discrete_sequence=[COLORS[0]])
                fig2.update_layout(yaxis_title="%")
                st.plotly_chart(fig2, use_container_width=True)

            max_pct = sizes["pct"].max()
            if max_pct <= 40:
                st.success(f"Good balance — largest cluster is {max_pct}% (under 40% threshold).")
            else:
                st.warning(f"Largest cluster is {max_pct}% — may indicate an imbalanced segmentation.")

        # Segment separation
        st.subheader("Segment separation")
        st.markdown("Do the segments actually differ? Averages should be clearly distinct.")
        separation = q(f"""
            SELECT segment_name,
                   ROUND(AVG(val_trns), 0) AS avg_spend,
                   ROUND(AVG(nr_trns), 0) AS avg_txns,
                   ROUND(AVG(lst_trns_days), 0) AS avg_recency_days,
                   ROUND(AVG(avg_val), 0) AS avg_txn_value,
                   ROUND(AVG(active_destinations), 1) AS avg_merchants
            FROM `{PROJECT}.marts.mart_cluster_output`
            GROUP BY segment_name
            ORDER BY avg_spend DESC
        """)

        if not separation.empty:
            st.dataframe(separation.rename(columns={
                "segment_name": "Segment", "avg_spend": "Avg spend (R)",
                "avg_txns": "Avg txns", "avg_recency_days": "Avg recency (days)",
                "avg_txn_value": "Avg txn value (R)", "avg_merchants": "Avg merchants"
            }), use_container_width=True, hide_index=True)

        # Revenue concentration
        st.subheader("Revenue concentration")
        revenue = q(f"""
            SELECT segment_name,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_customers,
                   ROUND(SUM(val_trns) * 100.0 / SUM(SUM(val_trns)) OVER(), 1) AS pct_revenue
            FROM `{PROJECT}.marts.mart_cluster_output`
            GROUP BY segment_name
            ORDER BY pct_revenue DESC
        """)

        if not revenue.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(name="% of customers", x=revenue["segment_name"],
                                 y=revenue["pct_customers"], marker_color=COLORS[7]))
            fig.add_trace(go.Bar(name="% of revenue", x=revenue["segment_name"],
                                 y=revenue["pct_revenue"], marker_color=COLORS[0]))
            fig.update_layout(barmode="group", title="Revenue concentration by segment",
                              yaxis_title="%", height=400)
            st.plotly_chart(fig, use_container_width=True)

            champ = revenue[revenue["segment_name"] == "Champions"]
            if not champ.empty:
                c = champ.iloc[0]
                st.info(f"**Champions** are {c['pct_customers']}% of customers but drive {c['pct_revenue']}% of revenue.")

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- page 12: data health --

elif page == "🏥 Data Health":
    st.title("Data Health Check")

    try:
        health = q(f"""
            SELECT 'staging.stg_transactions' AS tbl, COUNT(*) AS n FROM `{PROJECT}.staging.stg_transactions`
            UNION ALL SELECT 'staging.stg_customers', COUNT(*) FROM `{PROJECT}.staging.stg_customers`
            UNION ALL SELECT 'analytics.int_rfm_features', COUNT(*) FROM `{PROJECT}.analytics.int_rfm_features`
            UNION ALL SELECT 'analytics.int_rfm_scores', COUNT(*) FROM `{PROJECT}.analytics.int_rfm_scores`
            UNION ALL SELECT 'analytics.int_customer_category_spend', COUNT(*) FROM `{PROJECT}.analytics.int_customer_category_spend`
            UNION ALL SELECT 'analytics.int_destination_metrics', COUNT(*) FROM `{PROJECT}.analytics.int_destination_metrics`
            UNION ALL SELECT 'marts.mart_cluster_output', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_output`
            UNION ALL SELECT 'marts.mart_cluster_profiles', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_profiles`
            UNION ALL SELECT 'marts.mart_cluster_summary', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_summary`
            UNION ALL SELECT 'marts.mart_behavioral_summary', COUNT(*) FROM `{PROJECT}.marts.mart_behavioral_summary`
            UNION ALL SELECT 'marts.mart_geo_summary', COUNT(*) FROM `{PROJECT}.marts.mart_geo_summary`
            UNION ALL SELECT 'marts.mart_churn_risk', COUNT(*) FROM `{PROJECT}.marts.mart_churn_risk`
            UNION ALL SELECT 'marts.mart_monthly_trends', COUNT(*) FROM `{PROJECT}.marts.mart_monthly_trends`
            UNION ALL SELECT 'marts.mart_demographic_summary', COUNT(*) FROM `{PROJECT}.marts.mart_demographic_summary`
            UNION ALL SELECT 'marts.mart_destination_benchmarks', COUNT(*) FROM `{PROJECT}.marts.mart_destination_benchmarks`
            ORDER BY tbl
        """)

        health["status"] = health["n"].apply(lambda x: "✅" if x > 0 else "❌")
        health["layer"] = health["tbl"].str.split(".").str[0]

        st.dataframe(health.rename(columns={
            "tbl": "Table", "n": "Row count", "status": "Status", "layer": "Layer"
        }), use_container_width=True, hide_index=True)

        if (health["status"] == "✅").all():
            st.success("All tables populated.")
        else:
            st.error("Some tables are empty. Run the pipeline: `bash scripts/run.sh`")

    except Exception as e:
        st.error(f"Query failed: {e}")


# -- footer --

st.sidebar.markdown("---")
st.sidebar.markdown("**FNB NAV Data Platform**")
st.sidebar.markdown("Built by Prosper Sikhwari")
