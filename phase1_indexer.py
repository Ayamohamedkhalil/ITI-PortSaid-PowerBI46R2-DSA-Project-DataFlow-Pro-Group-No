"""
Phase 1: The Query Optimizer (Sorting & Searching)
====================================================
DataFlow Pro – NileMart ETL Engine
Business Goal: Speed up data retrieval for daily sales fact tables.
"""

import random
import time
import bisect


# Data Generation

def generate_transactions(n: int = 10_000) -> list[dict]:
    """Generates n simulated NileMart transaction records."""
    branches = ["Maadi", "Zayed", "Smouha", "Mansoura", "Zamalek", "Heliopolis"]
    records = []
    for i in range(n):
        records.append({
            "txn_id":   random.randint(100_000, 999_999),
            "branch":   random.choice(branches),
            "amount":   round(random.uniform(50, 15_000), 2),
            "date_key": random.randint(20240101, 20241231),  # YYYYMMDD
        })
    return records


# Sorting 

def bubble_sort(data: list[dict], key: str = "txn_id") -> list[dict]:
    """O(n²)  Naive comparison sort. Included to demonstrate inefficiency."""
    arr = data[:]
    n = len(arr)
    for i in range(n):
        for j in range(n - i - 1):
            if arr[j][key] > arr[j + 1][key]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    return arr


def insertion_sort(data: list[dict], key: str = "txn_id") -> list[dict]:
    """O(n²)  Efficient on nearly-sorted data; still slow at scale."""
    arr = data[:]
    for i in range(1, len(arr)):
        current = arr[i]
        j = i - 1
        while j >= 0 and arr[j][key] > current[key]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = current
    return arr


def selection_sort(data: list[dict], key: str = "txn_id") -> list[dict]:
    """O(n²)  Always makes n² comparisons regardless of input."""
    arr = data[:]
    n = len(arr)
    for i in range(n):
        min_idx = i
        for j in range(i + 1, n):
            if arr[j][key] < arr[min_idx][key]:
                min_idx = j
        arr[i], arr[min_idx] = arr[min_idx], arr[i]
    return arr


def merge_sort(data: list[dict], key: str = "txn_id") -> list[dict]:
    """O(n log n) Divide-and-conquer. Stable sort, great for large datasets."""
    if len(data) <= 1:
        return data[:]
    mid = len(data) // 2
    left  = merge_sort(data[:mid],  key)
    right = merge_sort(data[mid:], key)
    return _merge(left, right, key)


def _merge(left: list, right: list, key: str) -> list:
    result, i, j = [], 0, 0
    while i < len(left) and j < len(right):
        if left[i][key] <= right[j][key]:
            result.append(left[i]); i += 1
        else:
            result.append(right[j]); j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result


def quick_sort(data: list[dict], key: str = "txn_id") -> list[dict]:
    """O(n log n) avg Fastest in practice due to cache efficiency."""
    arr = data[:]
    _quick_sort_inplace(arr, 0, len(arr) - 1, key)
    return arr


def _quick_sort_inplace(arr: list, low: int, high: int, key: str) -> None:
    if low < high:
        pivot_idx = _partition(arr, low, high, key)
        _quick_sort_inplace(arr, low, pivot_idx - 1, key)
        _quick_sort_inplace(arr, pivot_idx + 1, high, key)


def _partition(arr: list, low: int, high: int, key: str) -> int:
    pivot = arr[high][key]
    i = low - 1
    for j in range(low, high):
        if arr[j][key] <= pivot:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]
    arr[i + 1], arr[high] = arr[high], arr[i + 1]
    return i + 1


def timsort(data: list[dict], key: str = "txn_id") -> list[dict]:
    """Python's built-in sort (Timsort)  O(n log n), production-grade."""
    return sorted(data, key=lambda x: x[key])

# Searching

def linear_search(data: list[dict], target_id: int) -> dict | None:
    """O(n)  Scans every record. Works on unsorted data."""
    for record in data:
        if record["txn_id"] == target_id:
            return record
    return None


def binary_search(sorted_data: list[dict], target_id: int) -> dict | None:
    """O(log n)  Requires sorted data. Extremely fast on large tables."""
    low, high = 0, len(sorted_data) - 1
    while low <= high:
        mid = (low + high) // 2
        mid_id = sorted_data[mid]["txn_id"]
        if mid_id == target_id:
            return sorted_data[mid]
        elif mid_id < target_id:
            low = mid + 1
        else:
            high = mid - 1
    return None


def bisect_date_slice(sorted_data: list[dict],
                      start_date: int,
                      end_date: int) -> list[dict]:
    """
    Uses Python's bisect module to extract a date range in O(log n) time.
    Example: slice Q3 sales (20240701 → 20240930) from the full year.
    """
    keys = [r["date_key"] for r in sorted_data]
    left  = bisect.bisect_left(keys,  start_date)
    right = bisect.bisect_right(keys, end_date)
    return sorted_data[left:right]



# Benchmarking


def _time_sort(fn, data, label, sample_size=1000):
    """Helper: time a sorting function on a sample."""
    sample = data[:sample_size]
    start = time.perf_counter()
    fn(sample)
    elapsed = time.perf_counter() - start
    print(f"  {label:<30} → {elapsed:.6f}s  (n={sample_size})")
    return elapsed


def run_benchmark():
    print("\n" + "=" * 58)
    print("  PHASE 1  QUERY OPTIMIZER BENCHMARK")
    print("=" * 58)

    print("\n[1/3] Generating 10,000 transaction records...")
    data = generate_transactions(10_000)
    print(f"      Sample record: {data[0]}")

    #Sorting Benchmark
    print("\n[2/3] Sorting Benchmark (sample of 1,000 records)")
    print("-" * 58)
    _time_sort(bubble_sort,    data, "Bubble Sort    O(n²)")
    _time_sort(insertion_sort, data, "Insertion Sort O(n²)")
    _time_sort(selection_sort, data, "Selection Sort O(n²)")
    _time_sort(merge_sort,     data, "Merge Sort     O(n log n)")
    _time_sort(quick_sort,     data, "Quick Sort     O(n log n)")
    _time_sort(timsort,        data, "Timsort (built-in) ")

    #Search Benchmark 
    print("\n[3/3] Search Benchmark on full 10,000 records")
    print("-" * 58)
    sorted_data = timsort(data)
    fraud_target = random.choice(sorted_data)["txn_id"]
    print(f"  Searching for fraudulent TXN ID: {fraud_target}")

    t0 = time.perf_counter()
    res_linear = linear_search(data, fraud_target)
    t_linear = time.perf_counter() - t0
    print(f"  Linear Search (unsorted) → {t_linear:.6f}s | Found: {res_linear is not None}")

    t0 = time.perf_counter()
    res_binary = binary_search(sorted_data, fraud_target)
    t_binary = time.perf_counter() - t0
    print(f"  Binary Search (sorted)   → {t_binary:.6f}s | Found: {res_binary is not None}")

    speedup = t_linear / t_binary if t_binary > 0 else float("inf")
    print(f"   Binary Search is ~{speedup:.0f}x faster than Linear Search")

    # Bisect date slice 
    sorted_by_date = sorted(data, key=lambda x: x["date_key"])
    q3 = bisect_date_slice(sorted_by_date, 20240701, 20240930)
    print(f"\n  bisect Q3 Slice (Jul Sep 2024) → {len(q3):,} transactions extracted instantly")

    print("\n Phase 1 complete.\n")


if __name__ == "__main__":
    run_benchmark()
