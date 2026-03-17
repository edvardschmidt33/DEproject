import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# --------------------------------------------------
# Config
# --------------------------------------------------
CSV_PATH = "results.csv"          # change if needed
OUTPUT_DIR = "plots"
os.makedirs(OUTPUT_DIR, exist_ok=True)

plt.rcParams["figure.dpi"] = 130
plt.rcParams["savefig.bbox"] = "tight"

# --------------------------------------------------
# Load and clean data
# --------------------------------------------------
df = pd.read_csv(CSV_PATH)

# Remove failed runs if any
df = df[df["runtime_seconds"] != "FAILED"].copy()

# Convert numeric columns
df["run"] = pd.to_numeric(df["run"], errors="coerce")
df["workers"] = pd.to_numeric(df["workers"], errors="coerce")
df["executor_cores"] = pd.to_numeric(df["executor_cores"], errors="coerce")
df["runtime_seconds"] = pd.to_numeric(df["runtime_seconds"], errors="coerce")

# Keep only valid rows
df = df.dropna(subset=["run", "workers", "executor_cores", "runtime_seconds"])

# Helpful numeric version of dataset size
df["dataset_gb"] = df["dataset_size"].str.replace("GB", "", regex=False).astype(float)

# Average over repeated runs
group_cols = [
    "experiment",
    "test",
    "workers",
    "dataset_size",
    "dataset_gb",
    "executor_cores",
    "executor_memory",
]
avg = (
    df.groupby(group_cols, as_index=False)["runtime_seconds"]
      .mean()
      .sort_values(["experiment", "workers", "executor_cores", "dataset_gb"])
)

print("Averaged results:")
print(avg)

# --------------------------------------------------
# Helper
# --------------------------------------------------
def save_plot(filename):
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, filename))
    plt.close()

# --------------------------------------------------
# 1) Horizontal strong - Runtime vs workers
# fixed dataset = 5GB, workers = 1,2,3
# --------------------------------------------------
hs = avg[avg["experiment"] == "horizontal_strong"].sort_values("workers")

plt.figure()
plt.plot(hs["workers"], hs["runtime_seconds"], marker="o")
plt.xlabel("Number of workers")
plt.ylabel("Runtime (seconds)")
plt.title("Horizontal Strong Scaling: Runtime vs Workers")
plt.xticks(hs["workers"])
save_plot("1_horizontal_strong_runtime_vs_workers.png")

# --------------------------------------------------
# 2) Horizontal weak - Runtime vs workers (actual vs ideal diagonal)
# Ideal weak scaling = constant runtime (flat line)
# Since you asked for "actual line vs ideal diagonal", I include both:
#   - actual runtime
#   - ideal weak scaling (flat at first runtime)
#   - diagonal reference (linear growth), if your teacher literally wants a diagonal
# --------------------------------------------------
hw = avg[avg["experiment"] == "horizontal_weak"].sort_values("workers")

workers_hw = hw["workers"].to_numpy()
runtime_hw = hw["runtime_seconds"].to_numpy()

ideal_flat_hw = np.full_like(runtime_hw, runtime_hw[0], dtype=float)
ideal_diagonal_hw = runtime_hw[0] * workers_hw / workers_hw[0]

plt.figure()
plt.plot(workers_hw, runtime_hw, marker="o", label="Actual")
plt.plot(workers_hw, ideal_flat_hw, linestyle="--", label="Ideal weak scaling (flat)")
plt.plot(workers_hw, ideal_diagonal_hw, linestyle=":", label="Diagonal reference")
plt.xlabel("Number of workers")
plt.ylabel("Runtime (seconds)")
plt.title("Horizontal Weak Scaling: Runtime vs Workers")
plt.xticks(workers_hw)
plt.legend()
save_plot("2_horizontal_weak_actual_vs_ideal.png")

# --------------------------------------------------
# 3) Horizontal weak - runtime vs workers with proportional data
# ideally flat
# --------------------------------------------------
plt.figure()
plt.plot(workers_hw, runtime_hw, marker="o", label="Actual")
plt.plot(workers_hw, ideal_flat_hw, linestyle="--", label="Ideal (flat)")
plt.xlabel("Number of workers")
plt.ylabel("Runtime (seconds)")
plt.title("Horizontal Weak Scaling: Proportional Data per Worker")
plt.xticks(workers_hw)
plt.legend()
save_plot("3_horizontal_weak_flat_ideal.png")

# --------------------------------------------------
# 4) Vertical strong - Runtime vs cores
# fixed dataset = 5GB, workers fixed at 3, cores = 1,2
# --------------------------------------------------
vs = avg[avg["experiment"] == "vertical_strong"].sort_values("executor_cores")

cores_vs = vs["executor_cores"].to_numpy()
runtime_vs = vs["runtime_seconds"].to_numpy()

plt.figure()
plt.plot(cores_vs, runtime_vs, marker="o")
plt.xlabel("Executor cores")
plt.ylabel("Runtime (seconds)")
plt.title("Vertical Strong Scaling: Runtime vs Cores")
plt.xticks(cores_vs)
save_plot("4_vertical_strong_runtime_vs_cores.png")

# --------------------------------------------------
# 5) Vertical strong - Speedup vs cores (actual vs ideal)
# speedup = T1 / Tp
# ideal = p
# --------------------------------------------------
baseline_vs = runtime_vs[0]
speedup_vs = baseline_vs / runtime_vs
ideal_speedup_vs = cores_vs / cores_vs[0]

plt.figure()
plt.plot(cores_vs, speedup_vs, marker="o", label="Actual speedup")
plt.plot(cores_vs, ideal_speedup_vs, linestyle="--", label="Ideal speedup")
plt.xlabel("Executor cores")
plt.ylabel("Speedup")
plt.title("Vertical Strong Scaling: Speedup vs Cores")
plt.xticks(cores_vs)
plt.legend()
save_plot("5_vertical_strong_speedup_vs_cores.png")

# --------------------------------------------------
# 6) Vertical weak - Runtime vs cores with proportional data
# ideally flat
# --------------------------------------------------
vw = avg[avg["experiment"] == "vertical_weak"].sort_values("executor_cores")

cores_vw = vw["executor_cores"].to_numpy()
runtime_vw = vw["runtime_seconds"].to_numpy()
ideal_flat_vw = np.full_like(runtime_vw, runtime_vw[0], dtype=float)

plt.figure()
plt.plot(cores_vw, runtime_vw, marker="o", label="Actual")
plt.plot(cores_vw, ideal_flat_vw, linestyle="--", label="Ideal (flat)")
plt.xlabel("Executor cores")
plt.ylabel("Runtime (seconds)")
plt.title("Vertical Weak Scaling: Runtime vs Cores")
plt.xticks(cores_vw)
plt.legend()
save_plot("6_vertical_weak_runtime_vs_cores.png")

# --------------------------------------------------
# 7) Horizontal vs vertical speedup comparison
# Side-by-side bars
#
# Horizontal strong speedup:
#   T(1 worker) / T(p workers)
#
# Vertical strong speedup:
#   T(1 core) / T(p cores)
# --------------------------------------------------
baseline_hs = hs.loc[hs["workers"] == hs["workers"].min(), "runtime_seconds"].iloc[0]
hs_speedup = baseline_hs / hs["runtime_seconds"].to_numpy()
hs_labels = hs["workers"].astype(str).to_list()

baseline_vs = vs.loc[vs["executor_cores"] == vs["executor_cores"].min(), "runtime_seconds"].iloc[0]
vs_speedup = baseline_vs / vs["runtime_seconds"].to_numpy()
vs_labels = vs["executor_cores"].astype(str).to_list()

# To compare cleanly, use matching scale factors 1 and 2.
# Horizontal has 1,2,3 workers while vertical has 1,2 cores.
# We'll compare for scale factor 1 and 2 only.
hs_compare = hs[hs["workers"].isin([1, 2])].sort_values("workers")
vs_compare = vs[vs["executor_cores"].isin([1, 2])].sort_values("executor_cores")

hs_compare_speedup = baseline_hs / hs_compare["runtime_seconds"].to_numpy()
vs_compare_speedup = baseline_vs / vs_compare["runtime_seconds"].to_numpy()

labels = ["1x", "2x"]
x = np.arange(len(labels))
width = 0.36

plt.figure()
plt.bar(x - width/2, hs_compare_speedup, width, label="Horizontal strong")
plt.bar(x + width/2, vs_compare_speedup, width, label="Vertical strong")
plt.xticks(x, labels)
plt.xlabel("Scaling factor")
plt.ylabel("Speedup")
plt.title("Horizontal vs Vertical Strong Scaling Speedup")
plt.legend()
save_plot("7_horizontal_vs_vertical_speedup_comparison.png")

print(f"\nSaved 7 plots in: {OUTPUT_DIR}")