"""
Dhaka AQI Weekly Report Generator
===================================
Generates 5 plots:
  1. AQI over time  — rain events shaded blue, flood events shaded red
  2. Rainfall mm over time (dual-source)
  3. AQI vs Rainfall scatter (correlation)
  4. AQI: Rain vs Dry vs Flood bar chart
  5. River discharge over time with flood threshold line
"""

import os
import warnings
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np

warnings.filterwarnings("ignore")

CSV_FILE = "dhaka_air_quality.csv"
PLOT_DIR = "weekly_plots"
os.makedirs(PLOT_DIR, exist_ok=True)

FLOOD_DISCHARGE_THRESHOLD = 3000   # m³/s — must match main.py

# ── Colour palette ────────────────────────────────────────────────────────────
C_AQI      = "#D64045"
C_RAIN     = "#4A90D9"
C_FLOOD    = "#E8A838"
C_DRY      = "#5BAD8F"
C_RIVER    = "#2E86AB"
C_SCATTER  = "#7B4F9E"
SHADE_RAIN  = "#4A90D9"
SHADE_FLOOD = "#E8A838"

# ── Load data ─────────────────────────────────────────────────────────────────

def load() -> pd.DataFrame | None:
    if not os.path.isfile(CSV_FILE):
        print(f"[ERROR] {CSV_FILE} not found.")
        return None
    df = pd.read_csv(CSV_FILE, parse_dates=["datetime_bst"])
    if df.empty:
        print("[WARN] CSV is empty.")
        return None

    df.sort_values("datetime_bst", inplace=True)
    df.reset_index(drop=True, inplace=True)

    for col in ["aqi", "pm25", "pm10", "rainfall_mm_openmeteo",
                "rainfall_mm_owm", "river_discharge_m3s",
                "temperature_c", "humidity_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derived columns
    df["date"] = df["datetime_bst"].dt.normalize()
    df["is_rain"]  = df["rain_event"].str.strip().str.lower()  == "yes"
    df["is_flood"] = df["flood_event"].str.strip().str.lower() == "yes"

    # Best rainfall column: average of both sources where available
    rain_cols = [c for c in ["rainfall_mm_openmeteo", "rainfall_mm_owm"] if c in df.columns]
    if rain_cols:
        df["rainfall_mm"] = df[rain_cols].mean(axis=1)
    else:
        df["rainfall_mm"] = np.nan

    return df

# ── Shared style ──────────────────────────────────────────────────────────────

def style(ax, title, xlabel="", ylabel=""):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(labelsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.35)

def shade_events(ax, df_daily, col_flag, color, alpha=0.18):
    """Shade background on days where col_flag is True."""
    for _, row in df_daily.iterrows():
        if row.get(col_flag):
            ax.axvspan(row["date"] - pd.Timedelta(hours=12),
                       row["date"] + pd.Timedelta(hours=12),
                       color=color, alpha=alpha, linewidth=0)

def fmt_xaxis(ax, df):
    span = (df["datetime_bst"].max() - df["datetime_bst"].min()).days
    if span <= 14:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")

# ── Plot 1: AQI over time with rain/flood shading ────────────────────────────

def plot_aqi_timeline(df):
    daily = df.groupby("date").agg(
        aqi=("aqi", "mean"),
        is_rain=("is_rain", "any"),
        is_flood=("is_flood", "any"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(12, 4.5))
    shade_events(ax, daily, "is_rain",  SHADE_RAIN,  alpha=0.20)
    shade_events(ax, daily, "is_flood", SHADE_FLOOD, alpha=0.30)

    ax.plot(daily["date"], daily["aqi"], color=C_AQI, linewidth=2.2,
            marker="o", markersize=4, label="Daily avg AQI", zorder=3)

    # AQI level bands
    for level, color, label in [
        (50,  "#2ECC71", "Good"),
        (100, "#F1C40F", "Moderate"),
        (150, "#E67E22", "Unhealthy for Sensitive"),
        (200, "#E74C3C", "Unhealthy"),
    ]:
        ax.axhline(level, linestyle=":", color=color, linewidth=0.9, alpha=0.7)

    # Legend patches
    patches = [
        mpatches.Patch(color=C_AQI,      label="Daily avg AQI"),
        mpatches.Patch(color=SHADE_RAIN,  alpha=0.5, label="Rain event"),
        mpatches.Patch(color=SHADE_FLOOD, alpha=0.6, label="Flood event"),
    ]
    ax.legend(handles=patches, frameon=False, fontsize=9)
    fmt_xaxis(ax, df)
    style(ax, "AQI Over Time — Rain & Flood Events Highlighted", ylabel="AQI")
    fig.tight_layout()
    save(fig, "01_aqi_timeline.png")

# ── Plot 2: Rainfall over time ────────────────────────────────────────────────

def plot_rainfall_timeline(df):
    if df["rainfall_mm"].isna().all():
        print("[SKIP] No rainfall data for plot 2.")
        return

    daily = df.groupby("date").agg(
        rainfall=("rainfall_mm", "sum"),
        is_flood=("is_flood", "any"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(12, 4))
    shade_events(ax, daily, "is_flood", SHADE_FLOOD, alpha=0.30)

    ax.bar(daily["date"], daily["rainfall"], color=C_RAIN,
           width=0.7, alpha=0.85, label="Rainfall (mm/day)")
    ax.axhline(0.5, linestyle="--", color="#888", linewidth=1, label="Rain threshold (0.5 mm)")

    fmt_xaxis(ax, df)
    ax.legend(frameon=False, fontsize=9)
    style(ax, "Daily Rainfall — Flood Events Highlighted",
          ylabel="Rainfall (mm)", xlabel="Date")
    fig.tight_layout()
    save(fig, "02_rainfall_timeline.png")

# ── Plot 3: AQI vs Rainfall scatter ──────────────────────────────────────────

def plot_aqi_vs_rain_scatter(df):
    sub = df[["aqi", "rainfall_mm", "is_flood"]].dropna()
    if sub.empty:
        print("[SKIP] Not enough data for scatter plot.")
        return

    fig, ax = plt.subplots(figsize=(7, 5))

    normal = sub[~sub["is_flood"]]
    floods = sub[sub["is_flood"]]

    ax.scatter(normal["rainfall_mm"], normal["aqi"],
               color=C_SCATTER, alpha=0.55, s=40, label="Normal")
    ax.scatter(floods["rainfall_mm"], floods["aqi"],
               color=C_FLOOD, alpha=0.85, s=70, marker="*",
               edgecolors="black", linewidths=0.4, label="Flood event", zorder=5)

    # Trend line
    if len(sub) >= 4:
        z = np.polyfit(sub["rainfall_mm"].fillna(0), sub["aqi"].fillna(0), 1)
        p = np.poly1d(z)
        xline = np.linspace(sub["rainfall_mm"].min(), sub["rainfall_mm"].max(), 100)
        ax.plot(xline, p(xline), color=C_AQI, linewidth=1.6,
                linestyle="--", label="Trend", zorder=4)

    ax.legend(frameon=False, fontsize=9)
    style(ax, "AQI vs Rainfall — Correlation",
          xlabel="Rainfall (mm)", ylabel="AQI")
    fig.tight_layout()
    save(fig, "03_aqi_vs_rainfall_scatter.png")

# ── Plot 4: AQI by condition (Dry / Rain / Flood) bar chart ──────────────────

def plot_aqi_by_condition(df):
    def label_condition(row):
        if row["is_flood"]: return "Flood"
        if row["is_rain"]:  return "Rain"
        return "Dry"

    df = df.copy()
    df["condition"] = df.apply(label_condition, axis=1)
    group = df.groupby("condition")["aqi"].mean().reindex(["Dry", "Rain", "Flood"]).dropna()

    if group.empty:
        print("[SKIP] No condition data for bar chart.")
        return

    color_map = {"Dry": C_DRY, "Rain": C_RAIN, "Flood": C_FLOOD}
    colors = [color_map[c] for c in group.index]

    fig, ax = plt.subplots(figsize=(6, 4.5))
    bars = ax.bar(group.index, group.values, color=colors,
                  edgecolor="white", width=0.45)
    for bar, val in zip(bars, group.values):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1.5, f"{val:.1f}",
                ha="center", va="bottom", fontsize=10, fontweight="bold")

    style(ax, "Average AQI: Dry vs Rain vs Flood Days",
          xlabel="Condition", ylabel="Average AQI")
    fig.tight_layout()
    save(fig, "04_aqi_by_condition.png")

# ── Plot 5: River discharge + flood threshold ─────────────────────────────────

def plot_river_discharge(df):
    if "river_discharge_m3s" not in df.columns or df["river_discharge_m3s"].isna().all():
        print("[SKIP] No river discharge data for plot 5.")
        return

    daily = df.groupby("date")["river_discharge_m3s"].mean().reset_index()

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(daily["date"], daily["river_discharge_m3s"],
                    color=C_RIVER, alpha=0.35)
    ax.plot(daily["date"], daily["river_discharge_m3s"],
            color=C_RIVER, linewidth=2, label="River discharge (m³/s)")
    ax.axhline(FLOOD_DISCHARGE_THRESHOLD, color=C_FLOOD, linewidth=1.8,
               linestyle="--", label=f"Flood threshold ({FLOOD_DISCHARGE_THRESHOLD} m³/s)")

    # Shade flood zones
    flood_days = df[df["is_flood"]]["date"].unique()
    for d in flood_days:
        ax.axvspan(pd.Timestamp(d) - pd.Timedelta(hours=12),
                   pd.Timestamp(d) + pd.Timedelta(hours=12),
                   color=SHADE_FLOOD, alpha=0.25, linewidth=0)

    ax.legend(frameon=False, fontsize=9)
    fmt_xaxis(ax, df)
    style(ax, "Buriganga River Discharge — Flood Events",
          ylabel="Discharge (m³/s)", xlabel="Date")
    fig.tight_layout()
    save(fig, "05_river_discharge.png")

# ── Save helper ───────────────────────────────────────────────────────────────

def save(fig, filename):
    path = os.path.join(PLOT_DIR, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Saved → {path}")

# ── Summary stats ─────────────────────────────────────────────────────────────

def print_summary(df):
    last7 = df[df["date"] >= df["date"].max() - pd.Timedelta(days=6)]
    print("\n── Weekly Summary ──────────────────────────────")
    print(f"  Total rows collected : {len(df)}")
    print(f"  Last 7 days rows     : {len(last7)}")
    print(f"  Avg AQI (7d)         : {last7['aqi'].mean():.1f}")
    print(f"  Rain events (7d)     : {last7['is_rain'].sum()} readings")
    print(f"  Flood events (7d)    : {last7['is_flood'].sum()} readings")
    print(f"  Avg rainfall (7d)    : {last7['rainfall_mm'].mean():.2f} mm")
    print("────────────────────────────────────────────────\n")

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("=== Dhaka AQI Weekly Report ===")
    df = load()
    if df is None:
        return
    print_summary(df)
    plot_aqi_timeline(df)
    plot_rainfall_timeline(df)
    plot_aqi_vs_rain_scatter(df)
    plot_aqi_by_condition(df)
    plot_river_discharge(df)
    print(f"\n=== All plots saved to ./{PLOT_DIR}/ ===")

if __name__ == "__main__":
    main()
