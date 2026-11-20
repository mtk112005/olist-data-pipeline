"""Read the analytics marts and render the README charts + a metrics JSON.

    python -m olist_pipeline.report
"""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .config import ROOT
from .warehouse import connect

OUT = ROOT / "docs" / "img"

QUERIES = {
    "delay_vs_review": """
        select case when delay_days <= 0 then 'on time'
                    when delay_days <= 3 then '1-3 days late'
                    when delay_days <= 7 then '4-7 days late'
                    else '8+ days late' end as bucket,
               count(*) as orders, avg(review_score) as avg_review,
               avg((review_score <= 2)::int) as share_1_2_star
        from core.fct_orders where is_delivered and review_score is not null
        group by 1""",
    "monthly": """
        select month, sum(delivered_orders) as orders,
               sum(late_rate * delivered_orders) / sum(delivered_orders) as late_rate
        from analytics.delivery_performance_monthly
        where month between '2017-01-01' and '2018-08-01'
        group by 1 order by 1""",
    "retention": """
        select months_since_first, sum(active_customers)::float / sum(cohort_size) as retention
        from analytics.customer_cohorts
        where months_since_first between 1 and 12 and cohort_month <= '2017-09-01'
        group by 1 order by 1""",
    "states": """
        select customer_state, sum(delivered_orders) as orders,
               sum(late_rate * delivered_orders) / sum(delivered_orders) as late_rate,
               sum(avg_delivery_days * delivered_orders) / sum(delivered_orders) as avg_days
        from analytics.delivery_performance_monthly group by 1 having sum(delivered_orders) > 500
        order by late_rate desc""",
    "headline": """
        select (select count(*) from core.fct_orders) as orders,
               (select count(*) from core.dim_customers) as customers,
               (select avg(is_repeat_customer::int) from core.dim_customers) as repeat_rate,
               (select avg(is_late::int) from core.fct_orders where is_delivered) as late_rate,
               (select avg(delivery_days) from core.fct_orders where is_delivered) as avg_delivery_days,
               (select avg(promised_days) from core.fct_orders where is_delivered) as avg_promised_days""",
}

ORDER = ["on time", "1-3 days late", "4-7 days late", "8+ days late"]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        d = {k: pd.read_sql(q, conn) for k, q in QUERIES.items()}

    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
    dv = d["delay_vs_review"].set_index("bucket").loc[ORDER]
    axes[0].bar(dv.index, dv.avg_review, color=["#2a9d8f", "#e9c46a", "#f4a261", "#e76f51"])
    for i, (r, s) in enumerate(zip(dv.avg_review, dv.share_1_2_star)):
        axes[0].text(i, r + 0.05, f"{r:.2f}\n({s:.0%} 1-2★)", ha="center", fontsize=8)
    axes[0].set(ylim=(0, 5), ylabel="Average review score", title="Late delivery destroys reviews")
    axes[0].tick_params(axis="x", labelsize=8)

    m = d["monthly"]
    ax = axes[1]
    ax.bar(pd.to_datetime(m.month), m.orders, width=20, color="#cfd8dc", label="delivered orders")
    ax2 = ax.twinx()
    ax2.plot(pd.to_datetime(m.month), m.late_rate * 100, color="#e76f51", marker="o", label="late %")
    ax.set(title="Volume vs. late-delivery rate", ylabel="Orders")
    ax2.set_ylabel("% delivered late")
    ax.tick_params(axis="x", rotation=45, labelsize=8)

    r = d["retention"]
    axes[2].plot(r.months_since_first, r.retention * 100, marker="o", color="#264653")
    axes[2].set(xlabel="Months since first order", ylabel="% of cohort ordering again",
                title="Repeat purchase is rare (<0.5%/month)")
    axes[2].grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "insights.png", dpi=130)
    plt.close(fig)

    h = d["headline"].iloc[0].to_dict()
    out = {"headline": {k: float(v) for k, v in h.items()},
           "delay_vs_review": dv.reset_index().to_dict("records"),
           "worst_states_by_late_rate": d["states"].head(5).to_dict("records")}
    (ROOT / "reports").mkdir(exist_ok=True)
    (ROOT / "reports" / "metrics.json").write_text(json.dumps(out, indent=2, default=float))
    print(json.dumps(out, indent=2, default=float))


if __name__ == "__main__":
    main()
