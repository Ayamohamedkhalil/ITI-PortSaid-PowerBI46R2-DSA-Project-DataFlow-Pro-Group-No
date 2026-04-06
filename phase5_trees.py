"""
Phase 5: The Hierarchical Matrix Builder (Trees)
=================================================
DataFlow Pro  NileMart ETL Engine
Business Goal: Model Parent-Child hierarchies for Power BI matrix visuals
               and enable roll-up aggregations.

Two structures are built:
  Part 1  Binary Search Tree  (Customer Dimension Index)
  Part 2  N-ary Tree          (Organizational Chart + Revenue Roll-Up)

Note: anytree is implemented manually here so the project has zero
      external dependencies. The API mirrors anytree's Node interface.
"""


#Binary search

class BSTNode:
    """A single node in the Customer Dimension BST."""
    def __init__(self, national_id: int, name: str):
        self.national_id: int              = national_id
        self.name:        str              = name
        self.left:        "BSTNode | None" = None
        self.right:       "BSTNode | None" = None


class DimensionIndex:
    """
    Binary Search Tree that stores unique Customer IDs (National IDs).
    Mimics how Power BI compresses dimension tables for fast relationships.

    • insert()  O(log n) average
    • search()  O(log n) average
    • in_order_traversal()  O(n), returns sorted list
    """

    def __init__(self):
        self.root: BSTNode | None = None

  #insertion  

    def insert(self, national_id: int, name: str) -> None:
        """Insert a new customer. Duplicate IDs are ignored (dimension tables
        require unique keys  same as Power BI's 'Remove Duplicates' step)."""
        if self.root is None:
            self.root = BSTNode(national_id, name)
        else:
            self._insert_rec(self.root, national_id, name)

    def _insert_rec(self, node: BSTNode, nid: int, name: str) -> None:
        if nid < node.national_id:
            if node.left is None:
                node.left = BSTNode(nid, name)
            else:
                self._insert_rec(node.left, nid, name)
        elif nid > node.national_id:
            if node.right is None:
                node.right = BSTNode(nid, name)
            else:
                self._insert_rec(node.right, nid, name)
        # Duplicate: silently skip (enforce uniqueness)

    #Search

    def search(self, national_id: int) -> BSTNode | None:
        """O(log n) lookup. Returns the node or None if not found."""
        return self._search_rec(self.root, national_id)

    def _search_rec(self, node: BSTNode | None, nid: int) -> BSTNode | None:
        if node is None or node.national_id == nid:
            return node
        if nid < node.national_id:
            return self._search_rec(node.left, nid)
        return self._search_rec(node.right, nid)

    # traversal

    def in_order(self) -> list[BSTNode]:
        """Returns all nodes sorted by national_id ascending. O(n)."""
        result: list[BSTNode] = []
        self._in_order_rec(self.root, result)
        return result

    def _in_order_rec(self, node: BSTNode | None, result: list) -> None:
        if node:
            self._in_order_rec(node.left, result)
            result.append(node)
            self._in_order_rec(node.right, result)

    def height(self) -> int:
        """Returns tree height. O(n)."""
        return self._height_rec(self.root)

    def _height_rec(self, node: BSTNode | None) -> int:
        if node is None:
            return 0
        return 1 + max(self._height_rec(node.left), self._height_rec(node.right))



#part 2: N-ARY TREE  (Organizational Chart)
#custom implementation – mirrors anytree.Node API


class Node:
    """
    A generic tree node that can have any number of children (N-ary).
    API is intentionally close to the anytree.Node interface:
      Node("Name", parent=parent_node, sales=0)
    """
    def __init__(self, name: str, parent: "Node | None" = None, **attrs):
        self.name:     str              = name
        self.children: list["Node"]    = []
        self._parent:  "Node | None"   = None

        # Store arbitrary keyword attributes (e.g., sales=150000)
        for k, v in attrs.items():
            setattr(self, k, v)

        if parent is not None:
            self.parent = parent   # triggers setter

    @property
    def parent(self) -> "Node | None":
        return self._parent

    @parent.setter
    def parent(self, new_parent: "Node | None") -> None:
        if self._parent is not None:
            self._parent.children.remove(self)
        self._parent = new_parent
        if new_parent is not None:
            new_parent.children.append(self)

    def is_leaf(self) -> bool:
        return len(self.children) == 0

    def depth(self) -> int:
        d, node = 0, self
        while node._parent:
            d += 1
            node = node._parent
        return d

    def __repr__(self) -> str:
        return f"Node({self.name!r})"


def render_tree(root: Node) -> str:
    """
    Returns a printable string of the tree, similar to anytree's RenderTree.
    Uses Unicode box-drawing characters for a polished look.
    """
    lines = []

    def _render(node: Node, prefix: str, is_last: bool) -> None:
        connector = "└── " if is_last else "├── "
        sales_val = getattr(node, "sales", None)
        sales_str = f"  (Direct Sales: {sales_val:,} EGP)" if sales_val is not None else ""
        lines.append(f"{prefix}{connector}{node.name}{sales_str}")
        child_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(node.children):
            _render(child, child_prefix, i == len(node.children) - 1)

    sales_val = getattr(root, "sales", None)
    sales_str = f"  (Direct Sales: {sales_val:,} EGP)" if sales_val is not None else ""
    lines.append(f"{root.name}{sales_str}")
    for i, child in enumerate(root.children):
        _render(child, "", i == len(root.children) - 1)

    return "\n".join(lines)


class OrgChartAnalyzer:
    """
    Models NileMart's employee hierarchy as an N-ary tree.
    Supports:
      • display_chart()    pretty-print the org structure
      • roll_up_sales()    recursive aggregation from leaves to any manager
    """

    def __init__(self):
        #corporate Hierarchy 
        self.ceo = Node("Omar  (Global CEO)",  sales=0)

        self.vp_cairo = Node("Tarek (VP Cairo & Giza)",   parent=self.ceo,      sales=0)
        self.vp_alex  = Node("Salma (VP Alex & Delta)",   parent=self.ceo,      sales=0)

        # Store Managers
        mgr_maadi   = Node("Hany  (Store Mgr Maadi)",   parent=self.vp_cairo, sales=30000)
        mgr_zayed   = Node("Dina  (Store Mgr  Zayed)",   parent=self.vp_cairo, sales=25000)
        mgr_smouha  = Node("Ramy  (Store Mgr  Smouha)",  parent=self.vp_alex,  sales=20000)
        mgr_mans    = Node("Lara  (Store Mgr  Mansoura)",parent=self.vp_alex,  sales=15000)

        # sales Reps (leaf nodes – actual revenue generators)
        Node("Aya     (Maadi Rep 1)",    parent=mgr_maadi,  sales=150_000)
        Node("Fady    (Maadi Rep 2)",    parent=mgr_maadi,  sales=95_000)
        Node("Mahmoud (Zayed Rep 1)",    parent=mgr_zayed,  sales=270_000)
        Node("Sara    (Zayed Rep 2)",    parent=mgr_zayed,  sales=110_000)
        Node("Kareem  (Smouha Rep 1)",   parent=mgr_smouha, sales=180_000)
        Node("Nour    (Mansoura Rep 1)", parent=mgr_mans,   sales=120_000)
        Node("Yusuf   (Mansoura Rep 2)", parent=mgr_mans,   sales=85_000)

    def display_chart(self) -> None:
        """Prints the visual hierarchy to the console."""
        print("\n  [Org Chart] NileMart Corporate Hierarchy")
        print("  " + "─" * 54)
        print(render_tree(self.ceo))

    def roll_up_sales(self, node: Node) -> int:
        """
        Recursively calculates total revenue for any manager node.
        Sums the manager's own direct sales + every descendant's sales.

        Time Complexity: O(n) where n = subtree size
        """
        total = getattr(node, "sales", 0)
        for child in node.children:
            total += self.roll_up_sales(child)
        return total



def run_demo():
    print("\n" + "=" * 58)
    print("  PHASE 5 HIERARCHICAL MATRIX BUILDER DEMO")
    print("=" * 58)

   
    print("\n[Part 1] Customer Dimension Index (Binary Search Tree)")
    print("  " + "-" * 50)

    dim_index = DimensionIndex()
    customers = [
        (29350187, "Ahmed Samir"),
        (30801245, "Mona Hassan"),
        (27654932, "Khaled Adel"),
        (31200789, "Nadia Fawzy"),
        (28900654, "Omar Tarek"),
        (32010011, "Layla Mostafa"),
        (26780123, "Samy Essam"),
    ]

    print("  Inserting customers into BST...")
    for nid, name in customers:
        dim_index.insert(nid, name)
        print(f"     Inserted: {nid} → {name}")

    print(f"\n  BST Height: {dim_index.height()} levels")

    print("\n  In-Order Traversal (sorted by National ID):")
    for node in dim_index.in_order():
        print(f"    {node.national_id} | {node.name}")

    print("\n  Searching for National ID 30801245...")
    result = dim_index.search(30801245)
    if result:
        print(f"  Found: {result.national_id} → {result.name}")

    print("\n  Searching for National ID 99999999 (not in table)...")
    result = dim_index.search(99999999)
    print(f"  {'✅ Found' if result else '❌ Not found  no match in dimension table'}")

    # ── Org Chart Demo ──
    print("\n\n[Part 2] Organizational Chart & Revenue Roll-Up")
    print("  " + "-" * 50)

    org = OrgChartAnalyzer()
    org.display_chart()

    managers_to_analyse = [
        ("Tarek (VP Cairo & Giza)", org.vp_cairo),
        ("Salma (VP Alex & Delta)", org.vp_alex),
        ("Omar  (Global CEO)",      org.ceo),
    ]

    print("\n  [Roll-Up Sales Aggregation]")
    for label, mgr_node in managers_to_analyse:
        total = org.roll_up_sales(mgr_node)
        print(f"   {label:<30} → Total Revenue: {total:>12,} EGP")

    print("\n Phase 5 complete.\n")


if __name__ == "__main__":
    run_demo()
