"""
main.py  DataFlow Pro CLI
===========================
NileMart ETL Engine  Interactive Menu
Run this file to access all five pipeline phases interactively.

  Usage:
    python main.py              # Full interactive mode
    python main.py --demo-all   # Run all phase demos and exit
"""

import sys
import os

# Allow running from any working directory
sys.path.insert(0, os.path.dirname(__file__))

from phase1_indexer import generate_transactions, timsort, linear_search, binary_search, bisect_date_slice
from phase2_tracker import AppliedStepsTracker
from phase3_parser  import DAXEvaluator
from phase4_buffer  import LiveIngestionQueue
from phase5_trees   import DimensionIndex, OrgChartAnalyzer

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
buffer     = LiveIngestionQueue()
dim_index  = DimensionIndex()
org_chart  = OrgChartAnalyzer()

# Pre-populate the BST dimension index with sample customers
_SAMPLE_CUSTOMERS = [
    (29350187, "Ahmed Samir"), (30801245, "Mona Hassan"),
    (27654932, "Khaled Adel"), (31200789, "Nadia Fawzy"),
    (28900654, "Omar Tarek"),
]
for _nid, _name in _SAMPLE_CUSTOMERS:
    dim_index.insert(_nid, _name)



def _hr(char="─", width=54):
    print(f"  {char * width}")


def _section(title: str):
    print()
    _hr("═")
    print(f"   {title}")
    _hr("═")


# phase 1

def menu_phase1():
    _section("Phase 1  Query Optimizer  (Sorting & Searching)")
    print("  a) Run full sorting + searching benchmark")
    print("  b) Sort 1,000 transactions with Quick Sort and show top 5")
    print("  c) Slice Q3 2024 transactions using bisect")
    print("  d) Search for a specific Transaction ID")
    print("  0) Back")

    choice = input("\n  Select: ").strip().lower()

    if choice == "a":
        from phase1_indexer import run_benchmark
        run_benchmark()

    elif choice == "b":
        print("\n  Generating 1,000 random transactions...")
        data = generate_transactions(1_000)
        sorted_data = timsort(data)
        print("  Top 5 transactions by TXN ID (ascending):")
        for r in sorted_data[:5]:
            print(f"    TXN {r['txn_id']} | {r['branch']:<12} | {r['amount']:>10,.2f} EGP | Date: {r['date_key']}")

    elif choice == "c":
        print("\n  Generating 10,000 records and slicing Q3 (Jul–Sep 2024)...")
        data = generate_transactions(10_000)
        sorted_by_date = sorted(data, key=lambda x: x["date_key"])
        q3 = bisect_date_slice(sorted_by_date, 20240701, 20240930)
        total = sum(r["amount"] for r in q3)
        print(f"  Q3 records: {len(q3):,} transactions")
        print(f"  Q3 revenue: {total:,.2f} EGP")

    elif choice == "d":
        data = generate_transactions(10_000)
        sorted_data = timsort(data)
        target = sorted_data[len(sorted_data) // 2]["txn_id"]   # pick a known ID
        print(f"\n  Searching for TXN ID: {target}")
        res = binary_search(sorted_data, target)
        if res:
            print(f"  ✅ Found: {res}")
        else:
            print("  ❌ Not found")


# phase 2

def menu_phase2():
    _section("Phase 2  Applied Steps Tracker  (Linked Lists)")
    print("  a) Add a transformation step")
    print("  b) Undo last step")
    print("  c) Redo step")
    print("  d) Display full history")
    print("  e) Run full demo")
    print("  0) Back")

    choice = input("\n  Select: ").strip().lower()

    if choice == "a":
        step = input("  Step name (e.g. 'Removed Null Rows'): ").strip()
        if step:
            tracker.add_step(step)
        else:
            print("  (empty step ignored)")
    elif choice == "b":
        tracker.undo()
    elif choice == "c":
        tracker.redo()
    elif choice == "d":
        tracker.display()
    elif choice == "e":
        from phase2_tracker import run_demo
        run_demo()


# phase 3

def menu_phase3():
    _section("Phase 3 – DAX Formula Parser  (Stacks)")
    print("  a) Evaluate a Postfix (RPN) expression")
    print("  b) Evaluate an Infix expression  (auto-converts via Shunting-Yard)")
    print("  c) Validate parentheses in a DAX formula")
    print("  d) Run full demo")
    print("  0) Back")

    choice = input("\n  Select: ").strip().lower()

    if choice == "a":
        print("  Example: 15000 5000 + 2 *   → (15000 + 5000) * 2")
        expr = input("  Enter postfix expression: ").strip()
        if expr:
            try:
                result = dax_engine.evaluate_postfix(expr)
                print(f"  Result: {result:,.4f}")
            except Exception as e:
                print(f"  ❌ Error: {e}")

    elif choice == "b":
        print("  Example: ( 15000 + 5000 ) * 2   (use spaces around operators)")
        expr = input("  Enter infix expression: ").strip()
        if expr:
            try:
                postfix = dax_engine.infix_to_postfix(expr)
                result  = dax_engine.evaluate_postfix(postfix)
                print(f"  Postfix: {postfix}")
                print(f"  Result:  {result:,.4f}")
            except Exception as e:
                print(f"  ❌ Error: {e}")

    elif choice == "c":
        expr = input("  Enter DAX formula to validate: ").strip()
        if expr:
            ok, msg = dax_engine.validate_parentheses(expr)
            icon = "✅" if ok else "❌"
            print(f"  {icon} {msg}")

    elif choice == "d":
        from phase3_parser import run_demo
        run_demo()


#phase 4 

def menu_phase4():
    _section("Phase 4  Live Data Buffer  (Queues)")
    print("  a) Enqueue a simulated POS transaction")
    print("  b) Process a batch (dequeue 3 rows)")
    print("  c) Show buffer status")
    print("  d) Run performance benchmark")
    print("  e) Run full demo")
    print("  0) Back")

    choice = input("\n  Select: ").strip().lower()

    if choice == "a":
        import random
        branches = ["Maadi", "Zayed", "Smouha", "Mansoura", "Zamalek", "Heliopolis"]
        txn = {
            "txn":    random.randint(3000, 9999),
            "branch": random.choice(branches),
            "amt_egp": random.randint(100, 12000),
        }
        buffer.enqueue_row(txn)

    elif choice == "b":
        batch = buffer.process_batch(3)
        if batch:
            total = sum(r["amt_egp"] for r in batch)
            print(f"  Batch revenue: {total:,} EGP")
        else:
            print("  Buffer is empty  nothing to process.")

    elif choice == "c":
        print(f"  Buffer size : {len(buffer)} rows")
        front = buffer.peek_front()
        if front:
            print(f"  Next in line: TXN #{front.get('txn')} | {front.get('branch')}")
        print(f"  Empty?       {buffer.is_empty()}")

    elif choice == "d":
        from phase4_buffer import _benchmark_queues
        _benchmark_queues(5000)

    elif choice == "e":
        from phase4_buffer import run_demo
        run_demo()


# phase 5

def menu_phase5():
    _section("Phase 5  Hierarchical Matrix Builder  (Trees)")
    print("  a) Display NileMart Org Chart")
    print("  b) Roll-up sales for VP Cairo")
    print("  c) Roll-up sales for VP Alex")
    print("  d) Roll-up sales for Global CEO (total company)")
    print("  e) Search Customer BST by National ID")
    print("  f) Show all customers (BST in-order)")
    print("  g) Run full demo")
    print("  0) Back")

    choice = input("\n  Select: ").strip().lower()

    if choice == "a":
        org_chart.display_chart()

    elif choice in ("b", "c", "d"):
        node_map = {
            "b": ("Tarek (VP Cairo & Giza)", org_chart.vp_cairo),
            "c": ("Salma (VP Alex & Delta)", org_chart.vp_alex),
            "d": ("Omar  (Global CEO)",       org_chart.ceo),
        }
        label, node = node_map[choice]
        total = org_chart.roll_up_sales(node)
        print(f"\n  📊 Roll-Up Revenue for {label}")
        print(f"     Total: {total:,} EGP")

    elif choice == "e":
        raw = input("  Enter National ID (e.g. 30801245): ").strip()
        try:
            nid    = int(raw)
            result = dim_index.search(nid)
            if result:
                print(f"  ✅ Found: {result.national_id} → {result.name}")
            else:
                print("  ❌ Customer not found in dimension index.")
        except ValueError:
            print("  Invalid ID – must be numeric.")

    elif choice == "f":
        nodes = dim_index.in_order()
        print(f"\n  Customer Dimension Table ({len(nodes)} customers, sorted by ID):")
        for n in nodes:
            print(f"    {n.national_id} | {n.name}")

    elif choice == "g":
        from phase5_trees import run_demo
        run_demo()


# main

MENU_OPTIONS = {
    "1": ("Phase 1 Query Optimizer       (Sorting & Searching)", menu_phase1),
    "2": ("Phase 2  Applied Steps Tracker (Linked Lists)",        menu_phase2),
    "3": ("Phase 3  DAX Formula Parser    (Stacks)",              menu_phase3),
    "4": ("Phase 4  Live Data Buffer      (Queues)",              menu_phase4),
    "5": ("Phase 5  Hierarchical Matrix   (Trees)",               menu_phase5),
}


def main():
    # ── Demo-all shortcut ──────────────────────
    if "--demo-all" in sys.argv:
        from phase1_indexer import run_benchmark
        from phase2_tracker import run_demo as d2
        from phase3_parser  import run_demo as d3
        from phase4_buffer  import run_demo as d4
        from phase5_trees   import run_demo as d5
        run_benchmark()
        d2(); d3(); d4(); d5()
        print("\n All phases demonstrated successfully!")
        return

    # ── Interactive mode ──────────────────────
    print(BANNER)

    while True:
        print("\n  ┌─────────────────────────────────────────────┐")
        print("  │         DATAFLOW PRO    Main Menu          │")
        print("  ├─────────────────────────────────────────────┤")
        for key, (label, _) in MENU_OPTIONS.items():
            print(f"  │  {key}. {label}  │")
        print("  │  6. Run ALL phase demos                     │")
        print("  │  0. Exit                                    │")
        print("  └─────────────────────────────────────────────┘")

        choice = input("\n  Select an option: ").strip()

        if choice in MENU_OPTIONS:
            MENU_OPTIONS[choice][1]()

        elif choice == "6":
            from phase1_indexer import run_benchmark
            from phase2_tracker import run_demo as d2
            from phase3_parser  import run_demo as d3
            from phase4_buffer  import run_demo as d4
            from phase5_trees   import run_demo as d5
            run_benchmark()
            d2(); d3(); d4(); d5()
            print("\n All phases complete!")

        elif choice == "0":
            print("\n  Shutting down DataFlow Pro. Masalama! 👋\n")
            sys.exit(0)
        else:
            print("    Invalid choice  please try again.")


if __name__ == "__main__":
    main()
