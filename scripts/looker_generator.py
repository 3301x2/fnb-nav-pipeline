#!/usr/bin/env python3
"""
looker_generator.py
═══════════════════════════════════════════════════════════════
Generate Looker Studio dashboards for ANY combination of views.

Modes:
  --all-views          One dashboard per view (19 URLs)
  --dashboard TYPE     Pre-built themed dashboard
  --client X --cat Y   Client pitch with auto-anonymization
  --views v1 v2 v3     Custom dashboard from specific views
  --template ID        Clone an existing template

Dashboard types:
  executive      Executive summary + segments + churn + CLV
  sales          Pitch opportunities + benchmarks + spend share + scorecard
  retention      Cohort retention + churn explained + spend momentum
  category       Category scorecard + affinity + propensity
  client-deep    Full client analysis (all views, filtered)
  full           Everything (all 19 views)

Usage:
  python scripts/looker_generator.py --all-views
  python scripts/looker_generator.py --dashboard executive
  python scripts/looker_generator.py --dashboard sales
  python scripts/looker_generator.py --client "Adidas" --cat "Clothing & Apparel"
  python scripts/looker_generator.py --views v_churn_risk v_churn_explained v_spend_momentum
  python scripts/looker_generator.py --dashboard full --template YOUR_REPORT_ID
"""

import sys
import argparse
from urllib.parse import quote

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

PROJECT_ID = "fmn-sandbox"
DATASET = "marts"

# All available views
ALL_VIEWS = {
    # Core pipeline
    "v_executive_summary":   "Executive Summary KPIs",
    "v_customer_segments":   "Customer Segments",
    "v_cluster_profiles":    "Cluster Profiles",
    "v_cluster_summary":     "Cluster Summary & Actions",
    "v_spend_share":         "Spend Share & Benchmarks",
    "v_demographics":        "Demographics",
    "v_monthly_trends":      "Monthly Trends",
    "v_behavioral":          "Behavioral Patterns",
    "v_geo_insights":        "Geo Insights",
    "v_churn_risk":          "Churn Risk Summary",
    "v_churn_detail":        "Churn Detail (Customer-Level)",
    # New analytics
    "v_cohort_retention":    "Cohort Retention",
    "v_category_affinity":   "Category Affinity (Cross-Shopping)",
    "v_category_scorecard":  "Category Scorecard",
    "v_pitch_opportunities": "Pitch Opportunities",
    "v_spend_momentum":      "Spend Momentum",
    "v_churn_explained":     "Churn Explained (ML Feature Drivers)",
    "v_category_propensity": "Category Propensity (Next-Best)",
    "v_customer_clv":        "Customer Lifetime Value",
    # Per-client + audience marketplace (added April 2026)
    "v_client_segment_mix":      "Per-Client Segment Mix",
    "v_audience_catalog":        "Audience Marketplace (FNB-wide)",
    "v_audience_client_overlap": "Audience × Client Overlap",
    # Pre-joined dashboard views (one-stop per page)
    "v_dashboard_overview":      "Dashboard — Overview scorecards",
    "v_dashboard_segments":      "Dashboard — Segment definitions",
    "v_dashboard_churn":         "Dashboard — Churn + CLV + Momentum",
    "v_dashboard_client_pitch":  "Dashboard — Client pitch (benchmarks + loyalty + time)",
    "v_pitch_internal":          "Pitch — Internal (real names)",
    "v_pitch_external":          "Pitch — External (anonymised competitors)",
}

# Pre-built dashboard themes
DASHBOARDS = {
    "executive": {
        "name": "NAV — Executive Dashboard",
        "views": [
            "v_executive_summary", "v_customer_segments", "v_cluster_profiles",
            "v_churn_risk", "v_customer_clv", "v_monthly_trends",
        ],
        "desc": "High-level KPIs, segments, churn, CLV, trends"
    },
    "sales": {
        "name": "NAV — Sales & Pitch Dashboard",
        "views": [
            "v_pitch_opportunities", "v_spend_share", "v_category_scorecard",
            "v_demographics", "v_geo_insights", "v_monthly_trends",
        ],
        "desc": "Who to pitch, market share, category health, demographics"
    },
    "retention": {
        "name": "NAV — Retention & Churn Dashboard",
        "views": [
            "v_churn_risk", "v_churn_explained", "v_churn_detail",
            "v_spend_momentum", "v_cohort_retention", "v_customer_clv",
        ],
        "desc": "Churn risk, why they're leaving, spend trends, cohort health, CLV"
    },
    "category": {
        "name": "NAV — Category Intelligence",
        "views": [
            "v_category_scorecard", "v_category_affinity", "v_category_propensity",
            "v_spend_share", "v_demographics", "v_monthly_trends",
        ],
        "desc": "Category portfolio, cross-shopping, propensity, benchmarks"
    },
    "segments": {
        "name": "NAV — Customer Segmentation Deep Dive",
        "views": [
            "v_customer_segments", "v_cluster_profiles", "v_cluster_summary",
            "v_behavioral", "v_demographics", "v_customer_clv",
        ],
        "desc": "Segment profiles, behavior, demographics, lifetime value"
    },
    "geo": {
        "name": "NAV — Geographic Analysis",
        "views": [
            "v_geo_insights", "v_spend_share", "v_demographics",
            "v_monthly_trends",
        ],
        "desc": "Provincial and municipal spend, market penetration by region"
    },
    "full": {
        "name": "NAV — Complete Analytics Platform",
        "views": list(ALL_VIEWS.keys()),
        "desc": "Every view — the full monty"
    },
    # ONE ROBUST DASHBOARD — the recommended build for the team.
    # Covers every page needed for client pitches without bloating the report.
    # Uses pre-joined dashboard views where available so fewer blends in Looker.
    "robust": {
        "name": "NAV — Robust Client Pitch Dashboard",
        "views": [
            "v_dashboard_overview",       # Page 1: Overview KPIs
            "v_dashboard_client_pitch",   # Page 2: Client Pitch (benchmarks + loyalty + time)
            "v_pitch_external",           # Page 2 companion: anonymised competitor view
            "v_client_segment_mix",       # Page 3: Per-client segment distribution (THE FIX)
            "v_dashboard_segments",       # Page 3 companion: segment definitions
            "v_audience_catalog",         # Page 4: Audience Marketplace (FNB-wide)
            "v_audience_client_overlap",  # Page 4 companion: which audiences are MY customers in
            "v_dashboard_churn",          # Page 5: Churn + CLV + Momentum
            "v_monthly_trends",           # Page 6: Trends
            "v_geo_insights",             # Page 6 companion: Geo
        ],
        "desc": "Single dashboard, 6 pages: Overview, Client Pitch, Per-Client Segments, Audience Marketplace, Churn/CLV, Trends/Geo. Uses per-client mart to avoid identical numbers across clients."
    },
}


# ═══════════════════════════════════════════════════════════════
# URL BUILDERS
# ═══════════════════════════════════════════════════════════════

def build_url(views, report_name, template_id=None, project=None, dataset=None):
    """Build a Looker Studio Linking API URL for given views."""
    project = project or PROJECT_ID
    dataset = dataset or DATASET
    base = "https://lookerstudio.google.com/reporting/create?"
    params = []

    if template_id:
        params.append(f"c.reportId={template_id}")
        params.append("c.mode=edit")

    params.append(f"r.reportName={quote(report_name)}")

    for i, view_name in enumerate(views):
        alias = f"ds{i}"
        label = ALL_VIEWS.get(view_name, view_name)
        params.extend([
            f"ds.{alias}.connector=bigQuery",
            f"ds.{alias}.datasourceName={quote(label)}",
            f"ds.{alias}.projectId={project}",
            f"ds.{alias}.type=TABLE",
            f"ds.{alias}.datasetId={dataset}",
            f"ds.{alias}.tableId={view_name}",
        ])

    return base + "&".join(params)


def build_custom_query_url(sql, report_name, project=None):
    """Build URL with custom SQL query as data source."""
    project = project or PROJECT_ID
    base = "https://lookerstudio.google.com/reporting/create?"
    params = [
        f"r.reportName={quote(report_name)}",
        "ds.ds0.connector=bigQuery",
        f"ds.ds0.datasourceName={quote(report_name)}",
        f"ds.ds0.projectId={project}",
        "ds.ds0.type=CUSTOM_QUERY",
        f"ds.ds0.sql={quote(sql)}",
    ]
    return base + "&".join(params)


def build_client_pitch_url(client, category, project=None, dataset=None):
    """Client pitch with real names (internal use)."""
    project = project or PROJECT_ID
    dataset = dataset or DATASET

    views = [
        "v_spend_share", "v_monthly_trends", "v_demographics",
        "v_geo_insights", "v_category_scorecard",
    ]
    return build_url(views, f"NAV Pitch — {client} ({category})", project=project, dataset=dataset)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Generate Looker Studio dashboards")
    parser.add_argument("--template", "-t", help="Template report ID to clone")
    parser.add_argument("--project", "-p", default=PROJECT_ID, help=f"GCP project (default: {PROJECT_ID})")
    parser.add_argument("--dataset", "-d", default=DATASET, help=f"Dataset (default: {DATASET})")
    parser.add_argument("--dashboard", "-D", choices=list(DASHBOARDS.keys()), help="Pre-built dashboard type")
    parser.add_argument("--client", "-c", help="Client name for pitch dashboard")
    parser.add_argument("--cat", help="Category for pitch dashboard")
    parser.add_argument("--views", "-v", nargs="+", help="Specific views to include")
    parser.add_argument("--all-views", action="store_true", help="One URL per view (19 dashboards)")
    parser.add_argument("--list", "-l", action="store_true", help="List all available views and dashboards")

    args = parser.parse_args()

    print()
    print("═══════════════════════════════════════════════════")
    print("  Looker Studio Dashboard Generator")
    print(f"  Project: {args.project} | Dataset: {args.dataset}")
    print("═══════════════════════════════════════════════════")
    print()

    # ── List mode ──
    if args.list:
        print("  Available views (19):")
        print()
        for view, label in ALL_VIEWS.items():
            print(f"    {view:30s} {label}")
        print()
        print("  Pre-built dashboards:")
        print()
        for key, dash in DASHBOARDS.items():
            print(f"    --dashboard {key:12s} {dash['desc']}")
            print(f"    {'':26s} Views: {', '.join(dash['views'][:3])}...")
            print()
        return

    # ── All views mode ──
    if args.all_views:
        print(f"  Individual dashboards for all {len(ALL_VIEWS)} views:")
        print()
        for view, label in ALL_VIEWS.items():
            url = build_url([view], f"NAV — {label}", args.template, args.project, args.dataset)
            print(f"  {label}:")
            print(f"    {url}")
            print()
        return

    # ── Client pitch mode ──
    if args.client:
        category = args.cat or "Clothing & Apparel"
        url = build_client_pitch_url(args.client, category, args.project, args.dataset)
        print(f"  Client pitch: {args.client} in {category}")
        print(f"  URL: {url}")
        print()
        print("  Open → Save and share → Done")
        return

    # ── Pre-built dashboard mode ──
    if args.dashboard:
        dash = DASHBOARDS[args.dashboard]
        url = build_url(dash["views"], dash["name"], args.template, args.project, args.dataset)
        print(f"  Dashboard: {args.dashboard}")
        print(f"  {dash['desc']}")
        print(f"  Data sources: {len(dash['views'])}")
        print()
        for v in dash["views"]:
            print(f"    • {ALL_VIEWS.get(v, v)}")
        print()
        print(f"  URL:")
        print(f"  {url}")
        print()
        print("  Open → Build charts → Save and share → Done")
        return

    # ── Custom views mode ──
    if args.views:
        valid = [v for v in args.views if v in ALL_VIEWS]
        invalid = [v for v in args.views if v not in ALL_VIEWS]
        if invalid:
            print(f"  ⚠ Unknown views: {', '.join(invalid)}")
            print(f"  Run --list to see available views")
            print()
        if valid:
            url = build_url(valid, "NAV — Custom Dashboard", args.template, args.project, args.dataset)
            print(f"  Custom dashboard with {len(valid)} views:")
            for v in valid:
                print(f"    • {ALL_VIEWS[v]}")
            print()
            print(f"  URL: {url}")
        return

    # ── Default: show all dashboard types ──
    print("  No mode selected. Available options:")
    print()
    print("  Quick start:")
    print("    python scripts/looker_generator.py --all-views")
    print("    python scripts/looker_generator.py --dashboard executive")
    print()
    print("  Pre-built dashboards:")
    for key, dash in DASHBOARDS.items():
        print(f"    --dashboard {key:12s} → {dash['name']}")
    print()
    print("  Client pitch:")
    print('    --client "Adidas" --cat "Clothing & Apparel"')
    print('    --client "Pick n Pay" --cat "Groceries"')
    print('    --client "Shell" --cat "Fuel & Energy"')
    print()
    print("  Custom:")
    print("    --views v_churn_risk v_churn_explained v_spend_momentum")
    print()
    print("  List everything:")
    print("    --list")
    print()

    # Show example URLs for all dashboard types
    print("  ─── Example URLs ───")
    print()
    for key, dash in DASHBOARDS.items():
        url = build_url(dash["views"], dash["name"], args.template, args.project, args.dataset)
        print(f"  {dash['name']}:")
        print(f"    {url}")
        print()


if __name__ == "__main__":
    main()
