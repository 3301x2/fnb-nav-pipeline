"""
generate_insights_pdf.py
pulls key insights from BigQuery and generates a clean PDF for non-technical stakeholders

usage:
    python scripts/generate_insights_pdf.py
    BQ_PROJECT=fmn-production python scripts/generate_insights_pdf.py

output: insights_report.pdf in the repo root

requires: pip install google-cloud-bigquery db-dtypes pandas matplotlib
"""

import os
import sys
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as mticker

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
LOCATION = 'africa-south1'
client = bigquery.Client(project=PROJECT, location=LOCATION)
OUTPUT = 'insights_report.pdf'

BLUE = '#2E75B6'
GRAY = '#607D8B'
GREEN = '#4CAF50'
RED = '#f44336'
ORANGE = '#FF9800'

def q(sql):
    return client.query(sql).to_dataframe()

def fmt_r(val):
    if val >= 1e9: return f'R{val/1e9:.1f}B'
    if val >= 1e6: return f'R{val/1e6:.1f}M'
    if val >= 1e3: return f'R{val/1e3:.0f}k'
    return f'R{val:.0f}'

def add_title_page(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    ax.text(0.5, 0.65, 'FNB NAV Data Platform', fontsize=32, fontweight='bold',
            ha='center', va='center', color=BLUE)
    ax.text(0.5, 0.55, 'Analytics Insights Report', fontsize=20,
            ha='center', va='center', color=GRAY)
    ax.text(0.5, 0.40, f'Generated: {datetime.now().strftime("%d %B %Y")}', fontsize=14,
            ha='center', va='center', color=GRAY)
    ax.text(0.5, 0.35, f'Project: {PROJECT}', fontsize=12,
            ha='center', va='center', color=GRAY)
    ax.text(0.5, 0.10, 'Confidential — Internal Use Only', fontsize=10,
            ha='center', va='center', color=RED, style='italic')
    pdf.savefig(fig)
    plt.close()

def add_pipeline_overview(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    ax.text(0.5, 0.95, 'Pipeline Overview', fontsize=24, fontweight='bold',
            ha='center', va='top', color=BLUE)

    try:
        overview = q(f"""
            SELECT 'Transactions' AS metric, FORMAT('%\\'d', COUNT(*)) AS value FROM `{PROJECT}.staging.stg_transactions`
            UNION ALL SELECT 'Customers', FORMAT('%\\'d', COUNT(*)) FROM `{PROJECT}.staging.stg_customers`
            UNION ALL SELECT 'Customers segmented', FORMAT('%\\'d', COUNT(*)) FROM `{PROJECT}.marts.mart_cluster_output`
            UNION ALL SELECT 'Customers scored (churn)', FORMAT('%\\'d', COUNT(*)) FROM `{PROJECT}.marts.mart_churn_risk`
            UNION ALL SELECT 'Destinations benchmarked', FORMAT('%\\'d', COUNT(*)) FROM `{PROJECT}.marts.mart_destination_benchmarks`
        """)
        y = 0.80
        for _, row in overview.iterrows():
            ax.text(0.3, y, row['metric'], fontsize=14, ha='left', va='center')
            ax.text(0.7, y, row['value'], fontsize=14, fontweight='bold', ha='left', va='center', color=BLUE)
            y -= 0.08
    except Exception as e:
        ax.text(0.5, 0.5, f'Could not load overview: {e}', fontsize=12, ha='center', color=RED)

    pdf.savefig(fig)
    plt.close()

def add_segments(pdf):
    fig, axes = plt.subplots(1, 2, figsize=(11, 8.5))
    fig.suptitle('Customer Segments', fontsize=24, fontweight='bold', color=BLUE, y=0.98)

    try:
        segs = q(f"""
            SELECT segment_name, customer_count, pct_of_total, avg_total_spend
            FROM `{PROJECT}.marts.mart_cluster_profiles`
            ORDER BY avg_total_spend DESC
        """)

        if not segs.empty:
            colors = [BLUE, GREEN, ORANGE, GRAY, RED]

            # pie chart
            axes[0].pie(segs['customer_count'], labels=segs['segment_name'],
                       autopct='%1.1f%%', colors=colors, startangle=90)
            axes[0].set_title('Customer distribution', fontsize=12, fontweight='bold')

            # bar chart avg spend
            bars = axes[1].barh(segs['segment_name'], segs['avg_total_spend'], color=colors)
            axes[1].set_xlabel('Avg total spend (R)')
            axes[1].set_title('Average spend per segment', fontsize=12, fontweight='bold')
            axes[1].invert_yaxis()
            axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: fmt_r(x)))

    except Exception as e:
        axes[0].text(0.5, 0.5, f'Error: {e}', ha='center')
        axes[0].axis('off')
        axes[1].axis('off')

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig)
    plt.close()

def add_revenue_concentration(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle('Revenue Concentration', fontsize=24, fontweight='bold', color=BLUE, y=0.98)

    try:
        rev = q(f"""
            SELECT segment_name,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_customers,
                   ROUND(SUM(val_trns) * 100.0 / SUM(SUM(val_trns)) OVER(), 1) AS pct_revenue
            FROM `{PROJECT}.marts.mart_cluster_output`
            GROUP BY segment_name
            ORDER BY pct_revenue DESC
        """)

        if not rev.empty:
            x_pos = range(len(rev))
            w = 0.35
            bars1 = ax.bar([p - w/2 for p in x_pos], rev['pct_customers'], w, label='% of customers', color=GRAY)
            bars2 = ax.bar([p + w/2 for p in x_pos], rev['pct_revenue'], w, label='% of revenue', color=BLUE)
            ax.set_xticks(x_pos)
            ax.set_xticklabels(rev['segment_name'], rotation=15)
            ax.set_ylabel('%')
            ax.legend()

            champ = rev[rev['segment_name'] == 'Champions'].iloc[0]
            ax.set_title(f'Champions are {champ["pct_customers"]}% of customers but drive {champ["pct_revenue"]}% of revenue',
                        fontsize=13, pad=15)

    except Exception as e:
        ax.text(0.5, 0.5, f'Error: {e}', ha='center', transform=ax.transAxes)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    pdf.savefig(fig)
    plt.close()

def add_churn_risk(pdf):
    fig, axes = plt.subplots(1, 2, figsize=(11, 8.5))
    fig.suptitle('Churn Risk Analysis', fontsize=24, fontweight='bold', color=BLUE, y=0.98)

    try:
        risk = q(f"""
            SELECT churn_risk_level,
                   COUNT(*) AS customers,
                   ROUND(SUM(total_spend), 0) AS total_spend
            FROM `{PROJECT}.marts.mart_churn_risk`
            GROUP BY churn_risk_level
            ORDER BY CASE churn_risk_level
                WHEN 'Critical' THEN 1 WHEN 'High' THEN 2
                WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 ELSE 5 END
        """)

        if not risk.empty:
            risk_colors = {'Critical': RED, 'High': ORANGE, 'Medium': '#fbc02d',
                          'Low': GREEN, 'Stable': BLUE}
            colors = [risk_colors.get(r, GRAY) for r in risk['churn_risk_level']]

            axes[0].pie(risk['customers'], labels=risk['churn_risk_level'],
                       autopct='%1.1f%%', colors=colors, startangle=90)
            axes[0].set_title('Customers by risk level', fontsize=12, fontweight='bold')

            axes[1].barh(risk['churn_risk_level'], risk['total_spend'], color=colors)
            axes[1].set_xlabel('Total spend (R)')
            axes[1].set_title('Spend at risk', fontsize=12, fontweight='bold')
            axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: fmt_r(x)))

            crit_high = risk[risk['churn_risk_level'].isin(['Critical', 'High'])]
            total_at_risk = crit_high['total_spend'].sum()
            fig.text(0.5, 0.02, f'Critical + High risk: {fmt_r(total_at_risk)} in spend at risk',
                    fontsize=13, ha='center', fontweight='bold', color=RED)

    except Exception as e:
        axes[0].text(0.5, 0.5, f'Error: {e}', ha='center')
        axes[0].axis('off')
        axes[1].axis('off')

    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    pdf.savefig(fig)
    plt.close()

def add_category_health(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle('Category Health Overview', fontsize=24, fontweight='bold', color=BLUE, y=0.98)

    try:
        cats = q(f"""
            SELECT CATEGORY_TWO, total_spend, growth_pct, avg_churn_pct, health_status
            FROM `{PROJECT}.marts.mart_category_scorecard`
            WHERE growth_pct IS NOT NULL
            ORDER BY total_spend DESC
            LIMIT 20
        """)

        if not cats.empty:
            health_colors = {'Growing': GREEN, 'Stable': BLUE, 'Slowing': ORANGE, 'Declining': RED}
            colors = [health_colors.get(h, GRAY) for h in cats['health_status']]

            scatter = ax.scatter(cats['growth_pct'], cats['avg_churn_pct'],
                               s=cats['total_spend'] / cats['total_spend'].max() * 1000,
                               c=colors, alpha=0.7, edgecolors='white', linewidth=1)

            for _, row in cats.head(10).iterrows():
                ax.annotate(row['CATEGORY_TWO'][:20], (row['growth_pct'], row['avg_churn_pct']),
                           fontsize=7, ha='center', va='bottom')

            ax.axhline(y=cats['avg_churn_pct'].median(), color=GRAY, linestyle='--', alpha=0.4)
            ax.axvline(x=0, color=GRAY, linestyle='--', alpha=0.4)
            ax.set_xlabel('Growth % (recent 3m vs prior 3m)')
            ax.set_ylabel('Avg churn risk %')
            ax.set_title('Top 20 categories — growth vs churn risk (size = total spend)', fontsize=12, pad=10)

            # legend
            for status, color in health_colors.items():
                ax.scatter([], [], c=color, s=60, label=status)
            ax.legend(loc='upper left', fontsize=9)

    except Exception as e:
        ax.text(0.5, 0.5, f'Error: {e}', ha='center', transform=ax.transAxes)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig)
    plt.close()

def add_top_opportunities(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    fig.suptitle('Top 15 Pitch Opportunities', fontsize=24, fontweight='bold', color=BLUE, y=0.98)

    try:
        opps = q(f"""
            SELECT DESTINATION, CATEGORY_TWO, market_share_pct,
                   ROUND(addressable_market / 1e6, 1) AS addressable_m,
                   pitch_score, recommended_action
            FROM `{PROJECT}.marts.mart_pitch_opportunities`
            ORDER BY pitch_score DESC
            LIMIT 15
        """)

        if not opps.empty:
            headers = ['Destination', 'Category', 'Share %', 'Addressable (RM)', 'Score', 'Action']
            cell_text = []
            for _, row in opps.iterrows():
                cell_text.append([
                    str(row['DESTINATION'])[:25],
                    str(row['CATEGORY_TWO'])[:25],
                    f'{row["market_share_pct"]:.1f}%',
                    f'R{row["addressable_m"]:.0f}M',
                    f'{row["pitch_score"]:.1f}',
                    str(row['recommended_action']).split(' - ')[0]
                ])

            table = ax.table(cellText=cell_text, colLabels=headers, loc='center',
                            cellLoc='left', colWidths=[0.18, 0.20, 0.10, 0.14, 0.08, 0.15])
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1, 1.4)

            for i in range(len(headers)):
                table[0, i].set_facecolor(BLUE)
                table[0, i].set_text_props(color='white', fontweight='bold')

    except Exception as e:
        ax.text(0.5, 0.5, f'Error: {e}', ha='center', transform=ax.transAxes)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig)
    plt.close()

def add_cohort_retention(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle('Customer Retention by Cohort', fontsize=24, fontweight='bold', color=BLUE, y=0.98)

    try:
        milestones = q(f"""
            SELECT months_since_first,
                   ROUND(AVG(retention_pct), 1) AS avg_retention
            FROM `{PROJECT}.marts.mart_cohort_retention`
            WHERE cohort_size >= 1000
              AND months_since_first IN (1, 2, 3, 6, 9, 12)
            GROUP BY months_since_first
            ORDER BY months_since_first
        """)

        if not milestones.empty:
            bars = ax.bar(milestones['months_since_first'].astype(str),
                         milestones['avg_retention'], color=BLUE, width=0.6)

            for bar, val in zip(bars, milestones['avg_retention']):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                       f'{val:.1f}%', ha='center', fontsize=11, fontweight='bold')

            ax.set_xlabel('Months since first transaction')
            ax.set_ylabel('Average retention %')
            ax.set_title('What % of customers are still active after N months?', fontsize=13, pad=15)
            ax.set_ylim(0, 105)

    except Exception as e:
        ax.text(0.5, 0.5, f'Error: {e}', ha='center', transform=ax.transAxes)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig)
    plt.close()

def add_cross_sell(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    fig.suptitle('Top Cross-Sell Opportunities', fontsize=24, fontweight='bold', color=BLUE, y=0.98)

    try:
        affinity = q(f"""
            SELECT category_a, category_b, shared_customers, lift,
                   pct_a_also_shops_b
            FROM `{PROJECT}.marts.mart_category_affinity`
            WHERE lift > 1.2
            ORDER BY shared_customers DESC
            LIMIT 15
        """)

        if not affinity.empty:
            headers = ['Category A', 'Category B', 'Shared customers', 'Lift', '% A also in B']
            cell_text = []
            for _, row in affinity.iterrows():
                cell_text.append([
                    str(row['category_a'])[:30],
                    str(row['category_b'])[:30],
                    f'{int(row["shared_customers"]):,}',
                    f'{row["lift"]:.1f}x',
                    f'{row["pct_a_also_shops_b"]:.0f}%'
                ])

            table = ax.table(cellText=cell_text, colLabels=headers, loc='center',
                            cellLoc='left', colWidths=[0.25, 0.25, 0.16, 0.10, 0.14])
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1, 1.4)

            for i in range(len(headers)):
                table[0, i].set_facecolor(BLUE)
                table[0, i].set_text_props(color='white', fontweight='bold')

            fig.text(0.5, 0.05, 'Lift = how much more likely customers are to shop both categories vs random chance',
                    fontsize=10, ha='center', color=GRAY, style='italic')

    except Exception as e:
        ax.text(0.5, 0.5, f'Error: {e}', ha='center', transform=ax.transAxes)

    plt.tight_layout(rect=[0, 0.07, 1, 0.95])
    pdf.savefig(fig)
    plt.close()


def main():
    print(f'generating insights report from {PROJECT}...')

    with PdfPages(OUTPUT) as pdf:
        print('  title page')
        add_title_page(pdf)

        print('  pipeline overview')
        add_pipeline_overview(pdf)

        print('  customer segments')
        add_segments(pdf)

        print('  revenue concentration')
        add_revenue_concentration(pdf)

        print('  churn risk')
        add_churn_risk(pdf)

        print('  category health')
        add_category_health(pdf)

        print('  pitch opportunities')
        add_top_opportunities(pdf)

        print('  cohort retention')
        add_cohort_retention(pdf)

        print('  cross-sell opportunities')
        add_cross_sell(pdf)

    print(f'\nsaved: {OUTPUT}')
    print(f'9 pages, ready to share with stakeholders')


if __name__ == '__main__':
    main()
