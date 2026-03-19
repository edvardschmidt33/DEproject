import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys

# load data:
# pass CSV files as arguments, they get concatenated
#run like this: python3 plot_scaling.py benchmark_results_20260318_144135/results.csv benchmark_results_20260318_192803/results.csv

if len(sys.argv) < 2:
    print("pass the results csv files as arguments, e.g: python3 plot_scaling.py results1.csv results2.csv")
    sys.exit(1)

frames = []
for f in sys.argv[1:]:
    frames.append(pd.read_csv(f))
df = pd.concat(frames, ignore_index=True)

# drop failed runs
df = df[df["runtime_seconds"] != "FAILED"]
df["runtime_seconds"] = df["runtime_seconds"].astype(float)

# group by experiment + test, compute mean and std
grouped = df.groupby(["experiment", "test"]).agg(
    mean_runtime=("runtime_seconds", "mean"),
    std_runtime=("runtime_seconds", "std"),
    workers=("workers", "first"),
    executor_cores=("executor_cores", "first"),
    dataset_size=("dataset_size", "first"),
).reset_index()

# split into experiments
hs = grouped[grouped["experiment"] == "horizontal_strong"].sort_values("workers")
hw = grouped[grouped["experiment"] == "horizontal_weak"].sort_values("workers")
vs = grouped[grouped["experiment"] == "vertical_strong"].sort_values("executor_cores")
vw = grouped[grouped["experiment"] == "vertical_weak"].sort_values("executor_cores")

#plot 1: horizontal strong runtime
fig, ax = plt.subplots(figsize=(6, 4))
ax.errorbar(hs["workers"], hs["mean_runtime"], yerr=hs["std_runtime"],
            marker="o", capsize=4, color="#2e86ab")
ax.set_xlabel("Workers")
ax.set_ylabel("Runtime (s)")
ax.set_title("Horizontal Strong Scaling, Runtime")
ax.set_xticks(hs["workers"].values)
plt.tight_layout()
plt.savefig("plot1_hs_runtime.png", dpi=150)
plt.close()

#plot 2: horizontal strong speedup
baseline_hs = hs["mean_runtime"].iloc[0]
hs_speedup = baseline_hs / hs["mean_runtime"]
ideal_speedup = hs["workers"].values.astype(float)

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot(hs["workers"], hs_speedup.values, marker="o", color="#2e86ab", label="Actual")
ax.plot(hs["workers"], ideal_speedup, marker="s", linestyle="--", color="#aaa", label="Ideal")
ax.set_xlabel("Workers")
ax.set_ylabel("Speedup (T₁ / Tₙ)")
ax.set_title("Horizontal Strong Scaling, Speedup")
ax.set_xticks(hs["workers"].values)
ax.legend()
plt.tight_layout()
plt.savefig("plot2_hs_speedup.png", dpi=150)
plt.close()

#plot 3: horizontal weak runtime
fig, ax = plt.subplots(figsize=(6, 4))
ax.errorbar(hw["workers"], hw["mean_runtime"], yerr=hw["std_runtime"],
            marker="o", capsize=4, color="#a23b72")
ax.set_xlabel("Workers")
ax.set_ylabel("Runtime (s)")
ax.set_title("Horizontal Weak Scaling, Runtime")
ax.set_xticks(hw["workers"].values)
plt.tight_layout()
plt.savefig("plot3_hw_runtime.png", dpi=150)
plt.close()

#plot 4: vertical strong runtime
fig, ax = plt.subplots(figsize=(6, 4))
ax.errorbar(vs["executor_cores"], vs["mean_runtime"], yerr=vs["std_runtime"],
            marker="o", capsize=4, color="#f18f01")
ax.set_xlabel("Executor Cores")
ax.set_ylabel("Runtime (s)")
ax.set_title("Vertical Strong Scaling, Runtime")
ax.set_xticks(vs["executor_cores"].values)
plt.tight_layout()
plt.savefig("plot4_vs_runtime.png", dpi=150)
plt.close()

#plot 5: vertical strong speedup
baseline_vs = vs["mean_runtime"].iloc[0]
vs_speedup = baseline_vs / vs["mean_runtime"]
ideal_vs = vs["executor_cores"].values.astype(float)

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot(vs["executor_cores"], vs_speedup.values, marker="o", color="#f18f01", label="Actual")
ax.plot(vs["executor_cores"], ideal_vs, marker="s", linestyle="--", color="#aaa", label="Ideal")
ax.set_xlabel("Executor Cores")
ax.set_ylabel("Speedup (T₁ / Tₙ)")
ax.set_title("Vertical Strong Scaling, Speedup")
ax.set_xticks(vs["executor_cores"].values)
ax.legend()
plt.tight_layout()
plt.savefig("plot5_vs_speedup.png", dpi=150)
plt.close()

#plot 6: vertical weak runtime
fig, ax = plt.subplots(figsize=(6, 4))
ax.errorbar(vw["executor_cores"], vw["mean_runtime"], yerr=vw["std_runtime"],
            marker="o", capsize=4, color="#5c946e")
ax.set_xlabel("Executor Cores")
ax.set_ylabel("Runtime (s)")
ax.set_title("Vertical Weak Scaling, Runtime")
ax.set_xticks(vw["executor_cores"].values)
plt.tight_layout()
plt.savefig("plot6_vw_runtime.png", dpi=150)
plt.close()

#plot 7: horizontal vs vertical comparison
# compare strong scaling speedups side by side
hs_max_speedup = (baseline_hs / hs["mean_runtime"].iloc[-1])
vs_max_speedup = (baseline_vs / vs["mean_runtime"].iloc[-1])

fig, ax = plt.subplots(figsize=(5, 4))
bars = ax.bar(["Horizontal\n(1→3 workers)", "Vertical\n(1→2 cores)"],
              [hs_max_speedup, vs_max_speedup],
              color=["#2e86ab", "#f18f01"], width=0.5)
ax.axhline(y=1.0, color="#aaa", linestyle="--", label="No speedup")
ax.set_ylabel("Speedup")
ax.set_title("Horizontal vs Vertical Strong Scaling")
# add value labels on bars
for bar in bars:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, h + 0.02,
            f"{h:.2f}x", ha="center", va="bottom", fontsize=11)
ax.legend()
plt.tight_layout()
plt.savefig("plot7_comparison.png", dpi=150)
plt.close()

print("done, saved plot1 through plot7 as png files")