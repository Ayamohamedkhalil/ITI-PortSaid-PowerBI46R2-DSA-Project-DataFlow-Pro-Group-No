"""
main.py  DataFlow Pro CLI
===========================
NileMart ETL Engine Interactive Menu
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from phase1_indexer import (
    generate_transactions, timsort,
    linear_search, binary_search, bisect_date_slice
)
from phase2_tracker import AppliedStepsTracker
from phase3_parser  import DAXEvaluator

BANNER = r"""
  ____        _        _____ _               ____
 |  _ \  __ _| |_ __ _|  ___| | _____      |  _ \ _ __ ___
 | | | |/ _` | __/ _` | |_  | |/ _ \ \ /\ / / |_) | '__/ _ \
 | |_| | (_| | || (_| |  _| | | (_) \ V  V /|  __/| | | (_) |
 |____/ \__,_|\__\__,_|_|   |_|\___/ \_/\_/ |_|   |_|  \___/

  NileMart ETL Engine  |  DataFlow Pro  |  DSA Project
  ═══════════════════════════════════════════════════════
"""

tracker    = AppliedStepsTracker()
dax_engine = DAXEvaluator()


def menu_phase1():
    print("\n  Phase 1  Query Optimizer (Sorting & Searching)")
    print("  a) Run full benchmark")
    print("  b) Sort 1,000 transactions and show top 5")
    print("  c) Slice Q3 2024 using bisect")
    print("  0) Back")
    choice = input("\n  Select: ").strip().lower()
    if choice == "a":
        from phase1_indexer import run_benchmark
        run_benchmark()
    elif choice == "b":
        data = generate_transactions(1_000)
        sorted_data = timsort(data)
        for r in sorted_data[:5]:
            print(f"    TXN {r['txn_id']} | {r['branch']:<12} | {r['amount']:>10,.2f} EGP")
    elif choice == "c":
        data = generate_transactions(10_000)
        sorted_by_date = sorted(data, key=lambda x: x["date_key"])
        q3 = bisect_date_slice(sorted_by_date, 20240701, 20240930)
        print(f"  Q3: {len(q3):,} records | {sum(r['amount'] for r in q3):,.2f} EGP")


def menu_phase2():
    print("\n  Phase 2  Applied Steps Tracker (Linked Lists)")
    print("  a) Add step  b) Undo  c) Redo  d) Display  e) Full demo")
    print("  0) Back")
    choice = input("\n  Select: ").strip().lower()
    if choice == "a":
        step = input("  Step name: ").strip()
        if step: tracker.add_step(step)
    elif choice == "b": tracker.undo()
    elif choice == "c": tracker.redo()
    elif choice == "d": tracker.display()
    elif choice == "e":
        from phase2_tracker import run_demo
        run_demo()


def menu_phase3():
    print("\n  Phase 3  DAX Formula Parser (Stacks)")
    print("  a) Evaluate Postfix expression")
    print("  b) Evaluate Infix expression")
    print("  c) Validate parentheses")
    print("  d) Run full demo")
    print("  0) Back")
    choice = input("\n  Select: ").strip().lower()
    if choice == "a":
        print("  Example: 15000 5000 + 2 *")
        expr = input("  Postfix expression: ").strip()
        if expr:
            try:
                print(f"  Result: {dax_engine.evaluate_postfix(expr):,.4f}")
            except Exception as e:
                print(f"  Error: {e}")
    elif choice == "b":
        print("  Example: ( 15000 + 5000 ) * 2")
        expr = input("  Infix expression: ").strip()
        if expr:
            try:
                postfix = dax_engine.infix_to_postfix(expr)
                result  = dax_engine.evaluate_postfix(postfix)
                print(f"  Postfix: {postfix}")
                print(f"  Result : {result:,.4f}")
            except Exception as e:
                print(f"  Error: {e}")
    elif choice == "c":
        expr = input("  Formula to validate: ").strip()
        if expr:
            ok, msg = dax_engine.validate_parentheses(expr)
            print(f"  {'✅' if ok else '❌'} {msg}")
    elif choice == "d":
        from phase3_parser import run_demo
        run_demo()


def main():
    print(BANNER)

    while True:
        print("\n  Main Menu:")
        print("  1. Phase 1 Query Optimizer       (Sorting & Searching)")
        print("  2. Phase 2  Applied Steps Tracker (Linked Lists)")
        print("  3. Phase 3  DAX Formula Parser    (Stacks)")
        print("  0. Exit")

        choice = input("\n  Select: ").strip()

        if   choice == "1": menu_phase1()
        elif choice == "2": menu_phase2()
        elif choice == "3": menu_phase3()
        elif choice == "0":
            print("\n  Shutting down DataFlow Pro. Masalama!\n")
            sys.exit(0)
        else:
            print("  Invalid choice.")

if __name__ == "__main__":
    main()
