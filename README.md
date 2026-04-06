# DataFlow Pro – NileMart ETL Engine

> A high-performance Python ETL pipeline demonstrating core Data Structures & Algorithms
> through a real-world Business Intelligence scenario.

---

## Project Structure

```
dataflow_pro/
├── data/                   # Mock CSV/JSON data (generated at runtime)
├── src/
│   ├── phase1_indexer.py   # Sorting (Bubble/Insertion/Selection/Merge/Quick/Tim) & Binary Search
│   ├── phase2_tracker.py   # Singly + Doubly Linked Lists (Undo/Redo engine)
│   ├── phase3_parser.py    # Array Stack + Linked Stack → Postfix evaluator + Shunting-Yard
│   ├── phase4_buffer.py    # ListQueue vs LinkedQueue vs collections.deque
│   ├── phase5_trees.py     # BST (Customer Dimension) + N-ary Org Chart + Roll-Up
│   └── main.py             # Interactive CLI
└── README.md
```

---

## Running the Project

```bash
# Interactive mode (full menu)
cd src
python main.py

# Run all five phase demos automatically and exit
python main.py --demo-all

# Run individual phases
python phase1_indexer.py
python phase2_tracker.py
python phase3_parser.py
python phase4_buffer.py
python phase5_trees.py
```

No external libraries required – pure Python 3.10+.

---

##  Big-O Performance Report

### Phase 1 – Sorting & Searching

| Algorithm      | Time Complexity | Notes |
|---------------|-----------------|-------|
| Bubble Sort   | O(n²)           | Swaps adjacent elements; extremely slow on 10k+ records |
| Insertion Sort | O(n²)          | Fast on nearly-sorted data; still impractical at scale |
| Selection Sort | O(n²)          | Always O(n²) regardless of input order |
| Merge Sort    | O(n log n)      | Stable, consistent; great for large datasets |
| Quick Sort    | O(n log n) avg  | Fastest in practice due to CPU cache locality |
| Timsort       | O(n log n)      | Python's built-in; hybrid Merge+Insertion; production champion |

**Why Quick Sort beats Bubble Sort:** At n=10,000, Bubble Sort performs ~50 million comparisons.
Quick Sort performs ~130,000 comparisons on average – a 380× reduction.

| Search        | Time Complexity | Requirement |
|--------------|-----------------|-------------|
| Linear Search | O(n)           | Works on unsorted data |
| Binary Search | O(log n)       | Requires sorted data; ~13 steps for 10,000 records |

### Phase 2 – Linked Lists

| Operation      | Singly LL | Doubly LL |
|---------------|-----------|-----------|
| Append        | O(n)      | O(1) with tail pointer |
| Undo (move back) | N/A    | O(1) via `prev` pointer |
| Redo (move forward) | N/A | O(1) via `next` pointer |

### Phase 3 – Stacks

Both `ArrayStack` and `LinkedStack` achieve **O(1)** push, pop, and peek.
The Array-based stack is faster in CPython due to memory locality.
The Linked-List stack demonstrates the DSA concept without amortized resizing.

### Phase 4 – Queues

| Implementation | Enqueue | Dequeue | Why |
|---------------|---------|---------|-----|
| ListQueue     | O(1)    | **O(n)**| `list.pop(0)` shifts all elements |
| LinkedQueue   | O(1)    | O(1)    | Head pointer removed directly |
| `deque`       | O(1)    | O(1)    | C-optimised doubly-linked list |

**Why `deque` over a standard list:** At 5,000 operations, `deque` is typically
10–50× faster than `list.pop(0)` because no element shifting occurs.

### Phase 5 – Trees

| Operation | BST (avg) | BST (worst) |
|-----------|-----------|-------------|
| Insert    | O(log n)  | O(n) – degenerate/sorted input |
| Search    | O(log n)  | O(n) |
| In-order  | O(n)      | O(n) |

Roll-Up traversal on the N-ary org tree: **O(n)** where n = subtree size.
