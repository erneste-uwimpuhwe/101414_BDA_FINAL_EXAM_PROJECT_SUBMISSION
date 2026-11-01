"""
Part 4 - static charts (PNG) from the result files in D:\\BDA_Project\\visuals
Run after: mongo_aggregations.py, spark_analysis.py, spark_sql.py, spark_clv.py, spark_viz_data.py
"""
import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
V = os.path.join(BASE, "visuals")
rd = lambda f: pd.read_csv(os.path.join(V, f))
plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": 0.3, "figure.dpi": 100})
BLUE, ORANGE, GREEN, GREY = "#2b6cb0", "#dd6b20", "#2f855a", "#718096"
made = []

def save(fig, name):
    path = os.path.join(V, name)
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    made.append(name)
    print("saved", path)

# ------------------------------------------------ 1. weekly revenue
w = rd("sql_weekly_revenue.csv")
w["week_start"] = pd.to_datetime(w["week_start"])
fig, ax = plt.subplots(figsize=(10, 4.8))
ax.plot(w["week_start"], w["revenue"] / 1e6, color=BLUE, marker="o", lw=2)
for i in (0, len(w) - 1):                      # first / last week are partial
    ax.plot(w["week_start"].iloc[i], w["revenue"].iloc[i] / 1e6, "o", mfc="white", mec=BLUE, ms=9)
ax.set_title("Weekly revenue, Aug-Oct 2026 (hollow markers = partial weeks)", loc="left", fontweight="bold")
ax.set_ylabel("Revenue (millions)")
ax.set_ylim(0, w["revenue"].max() / 1e6 * 1.2)
fig.autofmt_xdate()
save(fig, "chart1_weekly_revenue.png")

# ------------------------------------------------ 2. CLV tiers
t = rd("clv_tier_summary.csv").sort_values("clv_tier")
labels = [x.split("_", 1)[1] for x in t["clv_tier"]]
colors = [BLUE, GREEN, ORANGE, GREY, "#cbd5e0"][:len(t)]
fig, axs = plt.subplots(1, 2, figsize=(11, 4.8))
b = axs[0].bar(labels, t["avg_clv_12m_revenue"], color=colors)
for r, u in zip(b, t["users"]):
    axs[0].text(r.get_x() + r.get_width() / 2, r.get_height(), f"{r.get_height():,.0f}\n({u:,} users)",
                ha="center", va="bottom", fontsize=9)
axs[0].set_title("Projected 12-month revenue per customer", loc="left", fontweight="bold", fontsize=11)
axs[0].set_ylim(0, t["avg_clv_12m_revenue"].max() * 1.25)
b2 = axs[1].bar(labels, t["avg_sessions"], color=colors)
for r in b2:
    axs[1].text(r.get_x() + r.get_width() / 2, r.get_height(), f"{r.get_height():.1f}", ha="center", va="bottom", fontsize=9)
axs[1].set_title("Average browsing sessions per customer", loc="left", fontweight="bold", fontsize=11)
axs[1].set_ylim(0, t["avg_sessions"].max() * 1.25)
for a in axs:
    a.tick_params(axis="x", rotation=20)
pl, br = t[t["clv_tier"].str.contains("Platinum")].iloc[0], t[t["clv_tier"].str.contains("Bronze")].iloc[0]
fig.suptitle(f"Customer segments by lifetime value: Platinum earns {pl['avg_clv_12m_revenue'] / br['avg_clv_12m_revenue']:.1f}x "
             f"Bronze, with {pl['avg_sessions'] / br['avg_sessions']:.2f}x the browsing sessions",
             fontweight="bold", x=0.01, ha="left")
save(fig, "chart2_clv_tiers.png")

# ------------------------------------------------ 3. category revenue vs estimated profit
c = rd("sql_category_pareto.csv")
c["margin"] = c["est_profit"] / c["revenue"]
fig, ax = plt.subplots(figsize=(10, 5.6))
sc = ax.scatter(c["revenue"] / 1e6, c["est_profit"] / 1e3, c=c["margin"] * 100, cmap="viridis", s=90, edgecolor="white")
cb = fig.colorbar(sc, ax=ax); cb.set_label("Estimated profit margin (%)")
pick = {c["revenue"].idxmax(), c["est_profit"].idxmax(), c["est_profit"].idxmin(), c["margin"].idxmin()}
xmid = (c["revenue"].max() + c["revenue"].min()) / 2
for i in pick:
    right_half = c.loc[i, "revenue"] > xmid          # put labels of right-hand points on their left side
    ax.annotate(c.loc[i, "category_name"], (c.loc[i, "revenue"] / 1e6, c.loc[i, "est_profit"] / 1e3),
                xytext=(-10 if right_half else 10, -14 if right_half else 12), textcoords="offset points",
                ha="right" if right_half else "left", fontsize=9, arrowprops=dict(arrowstyle="-", color=GREY))
ax.margins(0.12)
ax.set_xlabel("Revenue (millions)"); ax.set_ylabel("Estimated profit (thousands)")
ax.set_title("Category performance: highest revenue is not highest profit", loc="left", fontweight="bold")
save(fig, "chart3_category_revenue_vs_profit.png")

# ------------------------------------------------ 4. conversion funnel
f = rd("funnel.csv")
fig, ax = plt.subplots(figsize=(10, 4.8))
y = np.arange(len(f))[::-1]
ax.barh(y, f["sessions"], color=BLUE, alpha=0.85)
for yi, (_, r) in zip(y, f.iterrows()):
    step = "" if pd.isna(r["pct_of_previous_stage"]) else f"  ({r['pct_of_previous_stage']:.1f}% of previous)"
    ax.text(r["sessions"], yi, f"  {int(r['sessions']):,}  |  {r['pct_of_all_sessions']:.1f}% of all{step}", va="center", fontsize=9)
ax.set_yticks(y); ax.set_yticklabels(f["stage"])
ax.set_xlim(0, f["sessions"].max() * 1.75)
ax.set_xlabel("Sessions"); ax.grid(axis="y", alpha=0)
ax.set_title("Conversion funnel: from visit to purchase", loc="left", fontweight="bold")
save(fig, "chart4_conversion_funnel.png")

# ------------------------------------------------ 5. product views vs units sold
p = rd("product_views_vs_sales.csv")
r = p["views"].corr(p["units_sold"])
fig, ax = plt.subplots(figsize=(10, 5.4))
ax.scatter(p["views"], p["units_sold"], alpha=0.3, s=14, color=BLUE)
for _, row in p.nlargest(3, "units_sold").iterrows():
    ax.annotate(row["product_id"], (row["views"], row["units_sold"]), xytext=(6, 6), textcoords="offset points", fontsize=9)
ax.set_xlabel("Product page views"); ax.set_ylabel("Units sold")
ax.set_title(f"Most-viewed vs top-selling products (all {len(p):,} products, correlation r = {r:.2f})",
             loc="left", fontweight="bold")
save(fig, "chart5_views_vs_sales.png")

# ------------------------------------------------ 6. cohort heatmap
co = rd("spark_cohort.csv")
m = co.pivot(index="registration_month", columns="month_index", values="active_pct")
fig, ax = plt.subplots(figsize=(10, 4.8))
im = ax.imshow(m.values, cmap="Blues", aspect="auto", vmin=0, vmax=max(80, np.nanmax(m.values)))
ax.set_xticks(range(m.shape[1])); ax.set_xticklabels(m.columns)
ax.set_yticks(range(m.shape[0])); ax.set_yticklabels(m.index)
for i in range(m.shape[0]):
    for j in range(m.shape[1]):
        if not np.isnan(m.values[i, j]):
            ax.text(j, i, f"{m.values[i, j]:.0f}%", ha="center", va="center", fontsize=9)
ax.grid(False)
ax.set_xlabel("Months since registration"); ax.set_ylabel("Registration cohort")
ax.set_title("Cohort activity: % of each cohort that bought in the month", loc="left", fontweight="bold")
fig.colorbar(im, ax=ax, label="% of cohort users")
save(fig, "chart6_cohort_heatmap.png")

print("\nCHARTS DONE:", ", ".join(made))
