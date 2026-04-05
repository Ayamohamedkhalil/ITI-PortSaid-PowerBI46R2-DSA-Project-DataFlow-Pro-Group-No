"""
Phase 3: The DAX Formula Parser (Stacks)
=========================================
DataFlow Pro  NileMart ETL Engine
Business Goal: Evaluate custom KPI formulas on the fly using a stack-based
               expression evaluator supporting postfix (RPN) notation and
               parenthesis validation for infix expressions.
"""



#Stack implementation :array based


class ArrayStack:
    """Stack backed by a Python list. O(1) push/pop amortised."""

    def __init__(self):
        self._data: list = []

    def push(self, item) -> None:
        self._data.append(item)

    def pop(self):
        if self.is_empty():
            raise IndexError("Stack underflow  nothing to pop")
        return self._data.pop()

    def peek(self):
        if self.is_empty():
            raise IndexError("Stack is empty")
        return self._data[-1]

    def is_empty(self) -> bool:
        return len(self._data) == 0

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"ArrayStack({self._data})"



#Stack implementation linked list based


class _StackNode:
    def __init__(self, value, next_node=None):
        self.value = value
        self.next  = next_node


class LinkedStack:
    """
    Stack backed by a singly linked list.
    Every push/pop is O(1) with zero dynamic array reallocation.
    """

    def __init__(self):
        self._top:  _StackNode | None = None
        self._size: int               = 0

    def push(self, item) -> None:
        self._top  = _StackNode(item, self._top)
        self._size += 1

    def pop(self):
        if self.is_empty():
            raise IndexError("Stack underflow – nothing to pop")
        value      = self._top.value
        self._top  = self._top.next
        self._size -= 1
        return value

    def peek(self):
        if self.is_empty():
            raise IndexError("Stack is empty")
        return self._top.value

    def is_empty(self) -> bool:
        return self._size == 0

    def __len__(self) -> int:
        return self._size

    def __repr__(self) -> str:
        items, current = [], self._top
        while current:
            items.append(current.value)
            current = current.next
        return f"LinkedStack({list(reversed(items))})"


# Dax

OPERATORS  = {"+", "-", "*", "/"}
PRECEDENCE = {"+": 1, "-": 1, "*": 2, "/": 2}


class DAXEvaluator:
    """
    Evaluates DAX-style arithmetic expressions.

    Supports two modes:
      1. evaluate_postfix(expr)  Evaluates a Reverse Polish Notation string.
         Example: "15000 5000 + 2 *"  →  (15000 + 5000) x 2  =  40000

      2. evaluate_infix(expr)   Converts infix to postfix (Shunting-Yard)
         then evaluates it.
         Example: "(Revenue - Cost) * Tax_Rate"  →  40000

      3. validate_parentheses(expr)  Checks that all brackets are balanced.
    """

    def __init__(self, use_linked_stack: bool = False):
        """
        use_linked_stack=False → Array-based stack (default, faster in CPython)
        use_linked_stack=True  → Linked-List stack (demonstrating DSA concept)
        """
        self._make_stack = LinkedStack if use_linked_stack else ArrayStack

 

    def evaluate_postfix(self, expression: str) -> float:
        """
        Postfix (RPN) evaluation using a single stack.
        Algorithm: Scan tokens left-to-right.
          • Number  → push
          • Operator → pop two operands, compute, push result
        O(n)
        """
        stack  = self._make_stack()
        tokens = expression.strip().split()

        for token in tokens:
            if self._is_number(token):
                stack.push(float(token))
            elif token in OPERATORS:
                if len(stack) < 2:
                    raise ValueError(f"Malformed expression near '{token}'")
                b = stack.pop()   # second operand (right)
                a = stack.pop()   # first  operand (left)
                stack.push(self._apply(a, token, b))
            else:
                raise ValueError(f"Unknown token: '{token}'")

        if len(stack) != 1:
            raise ValueError("Malformed expression  leftover operands on stack")
        return stack.pop()

    def infix_to_postfix(self, expression: str) -> str:
        """
        Shunting-Yard algorithm: converts infix to postfix.
        Handles: numbers, +  -  *  /  and parentheses.
        O(n)
        """
        output  = []
        op_stack = self._make_stack()
        tokens  = expression.replace("(", " ( ").replace(")", " ) ").split()

        for token in tokens:
            if self._is_number(token):
                output.append(token)
            elif token in OPERATORS:
                while (
                    not op_stack.is_empty()
                    and op_stack.peek() in OPERATORS
                    and PRECEDENCE[op_stack.peek()] >= PRECEDENCE[token]
                ):
                    output.append(op_stack.pop())
                op_stack.push(token)
            elif token == "(":
                op_stack.push(token)
            elif token == ")":
                while not op_stack.is_empty() and op_stack.peek() != "(":
                    output.append(op_stack.pop())
                if op_stack.is_empty():
                    raise ValueError("Mismatched parentheses  missing '('")
                op_stack.pop()  # discard '('
            else:
                raise ValueError(f"Unknown token: '{token}'")

        while not op_stack.is_empty():
            top = op_stack.pop()
            if top == "(":
                raise ValueError("Mismatched parentheses missing ')'")
            output.append(top)

        return " ".join(output)

    def evaluate_infix(self, expression: str) -> float:
        """Convert infix → postfix → evaluate. End-to-end convenience method."""
        postfix = self.infix_to_postfix(expression)
        return self.evaluate_postfix(postfix)

    def validate_parentheses(self, expression: str) -> tuple[bool, str]:
        """
        Checks that parentheses/brackets are balanced using a stack.
        Returns (True, 'OK') or (False, 'error message').
        O(n)
        """
        stack    = self._make_stack()
        pairs    = {")": "(", "]": "[", "}": "{"}
        openers  = set(pairs.values())

        for i, ch in enumerate(expression):
            if ch in openers:
                stack.push((ch, i))
            elif ch in pairs:
                if stack.is_empty():
                    return False, f"Unexpected '{ch}' at position {i}  no matching opener"
                top_ch, top_idx = stack.pop()
                if top_ch != pairs[ch]:
                    return False, (
                        f"Mismatched: '{top_ch}' at pos {top_idx} "
                        f"closed by '{ch}' at pos {i}"
                    )

        if not stack.is_empty():
            unclosed = [f"'{c}' at pos {i}" for c, i in [stack.pop() for _ in range(len(stack))]]
            return False, f"Unclosed brackets: {', '.join(unclosed)}"

        return True, "OK parentheses are balanced "

   

    @staticmethod
    def _is_number(token: str) -> bool:
        try:
            float(token)
            return True
        except ValueError:
            return False

    @staticmethod
    def _apply(a: float, op: str, b: float) -> float:
        if op == "+": return a + b
        if op == "-": return a - b
        if op == "*": return a * b
        if op == "/":
            if b == 0:
                raise ZeroDivisionError("Division by zero in DAX formula")
            return a / b
        raise ValueError(f"Unknown operator: {op}")

# Demo

def run_demo():
    print("\n" + "=" * 58)
    print("  PHASE 3 – DAX FORMULA PARSER DEMO")
    print("=" * 58)

    engine = DAXEvaluator()

    # ── Postfix evaluation ──
    print("\n[Postfix / RPN Evaluation]")
    cases = [
        ("15000 5000 + 2 *",    "(15000 + 5000) * 2   = Gross KPI"),
        ("120000 0.14 *",       "120000 * 0.14         = 14% VAT"),
        ("500000 120000 - 5 /", "(Revenue - Cost) / 5  = Avg Unit Margin"),
    ]
    for expr, label in cases:
        result = engine.evaluate_postfix(expr)
        print(f"  {label}")
        print(f"  Expression: {expr!r:40s} → Result: {result:,.2f} EGP\n")

    # ── Infix evaluation (Shunting-Yard) ──
    print("[Infix Evaluation via Shunting-Yard]")
    infix_cases = [
        "( 15000 + 5000 ) * 2",
        "500000 - 120000 * 0.14",
        "( 80000 + 40000 ) / ( 2 + 2 )",
    ]
    for expr in infix_cases:
        postfix = engine.infix_to_postfix(expr)
        result  = engine.evaluate_postfix(postfix)
        print(f"  Infix:   {expr}")
        print(f"  Postfix: {postfix}")
        print(f"  Result:  {result:,.2f} EGP\n")

    # ── Parenthesis validation ──
    print("[Parenthesis Validator]")
    test_exprs = [
        "(Revenue - Cost) * Tax",
        "((Revenue - Cost) * Tax",
        "(Revenue - Cost) * Tax)",
        "{[Revenue - Cost] * (Tax + Bonus)}",
    ]
    for expr in test_exprs:
        ok, msg = engine.validate_parentheses(expr)
        icon = "✅" if ok else "❌"
        print(f"  {icon} {expr!r}")
        if not ok:
            print(f"     ↳ {msg}")

    print("\n Phase 3 complete.\n")


if __name__ == "__main__":
    run_demo()
