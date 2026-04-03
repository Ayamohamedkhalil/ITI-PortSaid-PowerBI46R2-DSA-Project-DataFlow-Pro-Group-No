"""
phase 2 
"""

#single linked list

class SinglyNode:
    """A single node holding one ETL transformation step."""
    def __init__(self, step_name: str):
        self.step_name: str         = step_name
        self.next:      "SinglyNode | None" = None


class StepHistory:
    """
    Singly Linked List that appends transformation steps and lets the
    analyst traverse them in order (oldest → newest).
    """
    def __init__(self):
        self.head:  SinglyNode | None = None
        self._size: int               = 0

#core operations 
    def append(self, step_name: str) -> None:
        """Add a new step to the END of the history. O(n)."""
        new_node = SinglyNode(step_name)
        if self.head is None:
            self.head = new_node
        else:
            current = self.head
            while current.next:
                current = current.next
            current.next = new_node
        self._size += 1

    def display(self) -> None:
        """Print all steps in order."""
        if not self.head:
            print("  (no steps yet)")
            return
        current = self.head
        step_num = 1
        while current:
            connector = " → " if current.next else ""
            print(f"  Step {step_num}: [{current.step_name}]{connector}", end="")
            current = current.next
            step_num += 1
        print()

    def __len__(self) -> int:
        return self._size


#double linked list

class DoublyNode:
    """A node with both forward and backward pointers."""
    def __init__(self, step_name: str):
        self.step_name: str         = step_name
        self.next:      "DoublyNode | None" = None
        self.prev:      "DoublyNode | None" = None


class AppliedStepsTracker:
    """
    Doubly Linked List that mimics Power Query's Applied Steps panel.

    • add_step()   append a new transformation (O(1) with tail pointer)
    • undo()       move cursor backward one step  (O(1))
    • redo()       move cursor forward one step   (O(1))
    • display()    print full history with cursor position highlighted
    """

    def __init__(self):
        self.head:    DoublyNode | None = None
        self.tail:    DoublyNode | None = None   # O(1) append
        self.cursor:  DoublyNode | None = None   # current position
        self._size:   int               = 0

#core operation
    def add_step(self, step_name: str) -> None:
        """
        Appends a new step after the current cursor.
        If cursor is mid-history (after undo), future steps are discarded
        (same behavior as Power Query when you edit an earlier step).
        O(1)
        """
        new_node = DoublyNode(step_name)

        if self.head is None:
            # Empty list
            self.head = self.tail = self.cursor = new_node
        else:
            # Discard any steps ahead of cursor (they're now invalidated)
            if self.cursor and self.cursor.next:
                self._truncate_after_cursor()

            # Link new node after current tail / cursor
            new_node.prev = self.tail
            if self.tail:
                self.tail.next = new_node
            self.tail   = new_node
            self.cursor = new_node

        self._size += 1
        print(f"  [Tracker]  Added step: '{step_name}'")

    def undo(self) -> str | None:
        """Move cursor one step backward. O(1)."""
        if self.cursor is None or self.cursor.prev is None:
            print("  [Tracker]   Nothing to undo.")
            return None
        self.cursor = self.cursor.prev
        print(f"  [Tracker]  Undone. Now at: '{self.cursor.step_name}'")
        return self.cursor.step_name

    def redo(self) -> str | None:
        """Move cursor one step forward. O(1)."""
        if self.cursor is None or self.cursor.next is None:
            print("  [Tracker]  Nothing to redo.")
            return None
        self.cursor = self.cursor.next
        print(f"  [Tracker]   Redone. Now at: '{self.cursor.step_name}'")
        return self.cursor.step_name

    def display(self) -> None:
        """Print all steps, marking the current cursor position."""
        print("\n  [Applied Steps  Power Query History]")
        if not self.head:
            print("    (empty)")
            return
        current = self.head
        idx = 1
        while current:
            marker = "  CURRENT" if current is self.cursor else ""
            arrow  = " → " if current.next else ""
            print(f"    {idx}. [{current.step_name}]{marker}{arrow}", end="")
            current = current.next
            idx += 1
        print()

#private helpers
    def _truncate_after_cursor(self) -> None:
        """Discard all nodes after self.cursor (rewriting future history)."""
        node = self.cursor
        if node:
            node.next = None
            self.tail = node


#demo

def run_demo():
    print("\n" + "=" * 58)
    print("  PHASE 2  APPLIED STEPS TRACKER DEMO")
    print("=" * 58)

    tracker = AppliedStepsTracker()

    # Simulate a typical Power Query workflow 
    steps = [
        "Source  Connected to NileMart SQL Server",
        "Removed Null Rows",
        "Changed Type  amount_egp to Decimal",
        "Filtered Rows  branch != 'Test'",
        "Added Column  profit_margin",
        "Renamed Column  amt → amount_egp",
    ]

    print("\n-- Adding ETL transformation steps --")
    for s in steps:
        tracker.add_step(s)

    tracker.display()

    print("\n-- Undo 3 times --")
    tracker.undo()
    tracker.undo()
    tracker.undo()
    tracker.display()

    print("\n-- Redo 1 time --")
    tracker.redo()
    tracker.display()

    print("\n-- Add a new step (discards the 2 ahead) --")
    tracker.add_step("Merged Queries  joined with inventory table")
    tracker.display()

    print("\n Phase 2 complete.\n")


if __name__ == "__main__":
    run_demo()
