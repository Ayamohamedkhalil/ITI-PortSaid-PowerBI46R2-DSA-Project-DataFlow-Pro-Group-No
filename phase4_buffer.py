"""
Phase 4: The Live Data Buffer (Queues)
=======================================
DataFlow Pro  NileMart ETL Engine
Business Goal: Buffer live streaming sales data during peak events
               (e.g., White Friday) without crashing the server.

Three queue implementations are shown so you can compare performance:
  A. ListQueue       O(n) dequeue  (naive, for comparison)
  B. LinkedQueue     O(1) everywhere  (custom DSA implementation)
  C. LiveIngestionQueue  O(1) via collections.deque  (production-grade)
"""

from collections import deque




class ListQueue:
    """
    Uses a plain Python list as a queue.
      dequeue() calls list.pop(0) which is O(n) because every element
    must shift left by one index. At high throughput this kills performance.
    """

    def __init__(self):
        self._data: list = []

    def enqueue(self, item) -> None:
        """O(1)  append to the right end."""
        self._data.append(item)

    def dequeue(self):
        """O(n)  removes from the LEFT end (all elements shift)."""
        if self.is_empty():
            raise IndexError("Queue is empty")
        return self._data.pop(0)   # ← the performance trap!

    def peek(self):
        if self.is_empty():
            raise IndexError("Queue is empty")
        return self._data[0]

    def is_empty(self) -> bool:
        return len(self._data) == 0

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"ListQueue(size={len(self._data)}, front→{self._data[:3]}...)"



#implementation b: Linked-list uueue  (O(1) everywhere)


class _QNode:
    """Internal node for the LinkedQueue."""
    def __init__(self, data):
        self.data = data
        self.next: "_QNode | None" = None


class LinkedQueue:
    """
    FIFO queue backed by a singly linked list with head (front) and
    tail (back) pointers  both enqueue and dequeue are O(1).
    """

    def __init__(self):
        self._head: _QNode | None = None   #front of queue  (dequeue side)
        self._tail: _QNode | None = None   #back  of queue  (enqueue side)
        self._size: int           = 0

    def enqueue(self, item) -> None:
        """Append to the TAIL. O(1)."""
        node = _QNode(item)
        if self._tail:
            self._tail.next = node
        self._tail = node
        if self._head is None:
            self._head = node
        self._size += 1

    def dequeue(self):
        """Remove from the HEAD. O(1)."""
        if self.is_empty():
            raise IndexError("Queue is empty")
        value      = self._head.data
        self._head = self._head.next
        if self._head is None:
            self._tail = None   # queue is now empty
        self._size -= 1
        return value

    def peek(self):
        if self.is_empty():
            raise IndexError("Queue is empty")
        return self._head.data

    def is_empty(self) -> bool:
        return self._size == 0

    def __len__(self) -> int:
        return self._size

    def __repr__(self) -> str:
        items, cur = [], self._head
        for _ in range(min(3, self._size)):
            items.append(cur.data)
            cur = cur.next
        return f"LinkedQueue(size={self._size}, front→{items}...)"



#implementaion: deque-based  (production)


class LiveIngestionQueue:
    """
    Production-grade ingestion buffer for NileMart's live POS stream.
    Backed by collections.deque  all operations are O(1) in C-optimised code.

    Used during peak events (White Friday, Eid sales) when transactions
    arrive faster than the database can write them.
    """

    def __init__(self):
        self.buffer: deque = deque()

    def enqueue_row(self, row_data: dict) -> None:
        """Adds a new transaction row to the BACK of the buffer. O(1)."""
        self.buffer.append(row_data)
        print(f"  [Buffer]  Enqueued  TXN #{row_data.get('txn', '?')} "
              f"from {row_data.get('branch', '?')} "
              f"| Amount: {row_data.get('amt_egp', 0):,} EGP")

    def process_batch(self, batch_size: int) -> list:
        """
        Removes up to batch_size items from the FRONT of the buffer. O(batch_size).
        In production, this batch would be bulk-inserted into the Power BI dataset.
        """
        processed = []
        for _ in range(batch_size):
            if not self.buffer:
                break
            processed.append(self.buffer.popleft())  # O(1) !

        print(f"  [Buffer]  Processed {len(processed)} transactions "
              f"→ pushing to NileMart Power BI Datasets...")
        return processed

    def peek_front(self) -> dict | None:
        """Inspect the next item without removing it."""
        return self.buffer[0] if self.buffer else None

    def is_empty(self) -> bool:
        return len(self.buffer) == 0

    def __len__(self) -> int:
        return len(self.buffer)

    def __repr__(self) -> str:
        return f"LiveIngestionQueue(buffered={len(self.buffer)} rows)"



def _benchmark_queues(n: int = 5_000):
    """Compare enqueue+dequeue performance of all three implementations."""
    import time

    print(f"\n  [Benchmark] {n:,} enqueue + {n:,} dequeue operations")
    print("  " + "-" * 50)

    # List Queue
    q = ListQueue()
    t0 = time.perf_counter()
    for i in range(n): q.enqueue(i)
    for _ in range(n): q.dequeue()
    t_list = time.perf_counter() - t0
    print(f"  ListQueue  (O(n) dequeue) : {t_list:.4f}s")

    # Linked Queue
    q = LinkedQueue()
    t0 = time.perf_counter()
    for i in range(n): q.enqueue(i)
    for _ in range(n): q.dequeue()
    t_linked = time.perf_counter() - t0
    print(f"  LinkedQueue (O(1) both)   : {t_linked:.4f}s")

    # deque
    q = deque()
    t0 = time.perf_counter()
    for i in range(n): q.append(i)
    for _ in range(n): q.popleft()
    t_deque = time.perf_counter() - t0
    print(f"  deque (C-optimised O(1))  : {t_deque:.4f}s  🏆")

    speedup = t_list / t_deque if t_deque > 0 else float("inf")
    print(f"\n   deque is ~{speedup:.1f}x faster than the naive ListQueue")


# Demo

def run_demo():
    print("\n" + "=" * 58)
    print("  PHASE 4  LIVE DATA BUFFER DEMO")
    print("=" * 58)

    buffer = LiveIngestionQueue()

    print("\n-- Simulating White Friday POS stream from 6 branches --")
    live_transactions = [
        {"txn": 2001, "branch": "Maadi",      "amt_egp": 850},
        {"txn": 2002, "branch": "Smouha",     "amt_egp": 3200},
        {"txn": 2003, "branch": "Zayed",      "amt_egp": 1450},
        {"txn": 2004, "branch": "Mansoura",   "amt_egp": 670},
        {"txn": 2005, "branch": "Zamalek",    "amt_egp": 9800},
        {"txn": 2006, "branch": "Heliopolis", "amt_egp": 2150},
    ]

    for txn in live_transactions:
        buffer.enqueue_row(txn)

    print(f"\n  [Buffer]  Current buffer size: {len(buffer)} rows")
    print(f"  [Buffer]  Next in line: TXN #{buffer.peek_front()['txn']}")

    print("\n-- Processing in batches of 3 --")
    batch1 = buffer.process_batch(3)
    print(f"  Batch 1 total: {sum(r['amt_egp'] for r in batch1):,} EGP")

    batch2 = buffer.process_batch(3)
    print(f"  Batch 2 total: {sum(r['amt_egp'] for r in batch2):,} EGP")

    print(f"\n  [Buffer] Buffer empty: {buffer.is_empty()}")

    _benchmark_queues()

    print("\n Phase 4 complete.\n")


if __name__ == "__main__":
    run_demo()
