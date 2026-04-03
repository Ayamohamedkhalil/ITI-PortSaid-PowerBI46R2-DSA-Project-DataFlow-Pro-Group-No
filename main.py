"""
main.py – DataFlow Pro CLI
===========================
NileMart ETL Engine  Interactive Menu
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from phase1_indexer import (
    generate_transactions, timsort,
    linear_search, binary_search, bisect_date_slice
)

BANNER = r"""
  ____        _        _____ _               ____
 |  _ \  __ _| |_ __ _|  ___| | _____      |  _ \ _ __ ___
 | | | |/ _` | __/ _` | |_  | |/ _ \ \ /\ / / |_) | '__/ _ \
 | |_| | (_| | || (_| |  _| | | (_) \ V  V /|  __/| | | (_) |
 |____/ \__,_|\__\__,_|_|   |_|\___/ \_/\_/ |_|   |_|  \___/

  NileMart ETL Engine  |  DataFlow Pro  |  DSA Project
  ═══════════════════════════════════════════════════════
"""

def menu_phase1():
    print("\n  Phase 1  Query Optimizer (Sorting & Searching)")
    print("  a) Run full sorting + searching benchmark")
    print("  b) Sort 1,000 transactions with Quick Sort and show top 5")
    print("  c) Slice Q3 2024 transactions using bisect")
    print("  0) Back")

    choice = input("\n  Select: ").strip().lower()

    if choice == "a":
        from phase1_indexer import run_benchmark
        run_benchmark()

    elif choice == "b":
        print("\n  Generating 1,000 random transactions...")
        data = generate_transactions(1_000)
        sorted_data = timsort(data)
        print("  Top 5 transactions by TXN ID:")
        for r in sorted_data[:5]:
            print(f"    TXN {r['txn_id']} | {r['branch']:<12} | {r['amount']:>10,.2f} EGP")

    elif choice == "c":
        print("\n  Generating 10,000 records and slicing Q3 (Jul–Sep 2024)...")
        data = generate_transactions(10_000)
        sorted_by_date = sorted(data, key=lambda x: x["date_key"])
        q3 = bisect_date_slice(sorted_by_date, 20240701, 20240930)
        total = sum(r["amount"] for r in q3)
        print(f"  Q3 records : {len(q3):,} transactions")
        print(f"  Q3 revenue : {total:,.2f} EGP")


def main():
    print(BANNER)

    while True:
        print("\n  Main Menu:")
        print("  1. Phase 1  Query Optimizer  (Sorting & Searching)")
        print("  0. Exit")

        choice = input("\n  Select: ").strip()

        if choice == "1":
            menu_phase1()
        elif choice == "0":
            print("\n  Shutting down DataFlow Pro. Masalama!\n")
            sys.exit(0)
        else:
            print("  Invalid choice.")

if __name__ == "__main__":
    main()
