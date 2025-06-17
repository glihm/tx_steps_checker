# tx_steps_binned_plot_simple_labels.py
import sys
import numpy as np
import matplotlib.pyplot as plt

BIN_SIZE = 100_000  # Adjust bin size as needed

# Read data
with open(sys.argv[1]) as f:
    # Parse the new format: "txhash steps_count" or "txhash steps_count *"
    transactions = []
    for line in f:
        line = line.strip()
        if line:
            parts = line.split()
            if len(parts) >= 2:
                tx_hash = parts[0]
                steps = int(parts[1])
                transactions.append((tx_hash, steps))

# Extract just the step counts for numpy operations
steps = [tx[1] for tx in transactions]
arr = np.array(steps)

# Stats
print(f"Count     : {len(arr)}")
print(f"Mean      : {np.mean(arr):,.0f}")
print(f"Median    : {np.median(arr):,.0f}")
print(f"Min       : {np.min(arr):,}")
print(f"Max       : {np.max(arr):,}")
print(f"Q1 (25%)  : {np.percentile(arr, 25):,.0f}")
print(f"Q3 (75%)  : {np.percentile(arr, 75):,.0f}")
print(f"Std Dev   : {np.std(arr):,.0f}")

# Print top 10 largest transactions with their hashes
print("\nTop 10 largest transactions by step count:")
# Sort transactions by step count (descending) and take top 10
top_10_txs = sorted(transactions, key=lambda x: x[1], reverse=True)[:10]
for i, (tx_hash, steps) in enumerate(top_10_txs, 1):
    print(f"{i:2d}. {tx_hash} ({steps:,} steps)")

# Compute bins
min_step = int(np.min(arr) // BIN_SIZE * BIN_SIZE)
max_step = int(np.max(arr) // BIN_SIZE * BIN_SIZE + BIN_SIZE)
bins = np.arange(min_step, max_step + BIN_SIZE, BIN_SIZE)

# Histogram
hist, bin_edges = np.histogram(arr, bins=bins)

# X labels: bin start values, formatted in thousands (e.g., 100k, 200k)
labels = [f"{int(left/1000)}k" for left in bin_edges[:-1]]

# Plot
plt.figure(figsize=(12, 6))
plt.bar(labels, hist, width=1.0, edgecolor='black')
plt.title(f"Transaction Step Count Distribution (Bin = {BIN_SIZE:,} steps)")
plt.xlabel("Step Count (thousands)")
plt.ylabel("Number of Transactions")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
