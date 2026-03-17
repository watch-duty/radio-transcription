---
applyTo: "**/*.py"
---

Below is a style guide for the Python code in this project. Please follow these guidelines when contributing and reviewing code.

# Python Style Guide

## 1 Background

Python is the main dynamic language used. This style guide is a list of *dos and don'ts* for Python programs.

## 2 Python Language Rules

### 2.2 Imports

Use `import` statements for packages and modules only, not for individual types, classes, or functions.

**Decision:**

* Use `import x` for packages and modules.
* Use `from x import y` where `x` is the package prefix and `y` is the module name.
* Use `from x import y as z` if module names collide or the name is inconveniently long.
* Do not use relative names in imports. Use the full package name.

### 2.3 Packages

Import each module using the full pathname location of the module.

```python
import absl.flags
from doctor.who import jodie

```

### 2.4 Exceptions

Exceptions are allowed but must follow certain conditions:

* Make use of built-in exception classes (e.g., `ValueError`).
* Do not use `assert` statements for application logic. They should only verify expectations that, if removed, would not break the code.
* Never use catch-all `except:` statements or catch `Exception` unless re-raising or recording at an isolation point.
* Minimize the amount of code in a `try`/`except` block.

### 2.5 Mutable Global State

Avoid mutable global state. If warranted, prepending an `_` to the name to make it internal. Constants should be named using all caps with underscores (e.g., `MAX_COUNT = 3`).

### 2.6 Nested/Local/Inner Classes and Functions

Nested local functions or classes are fine when used to close over a local variable. Do not nest a function just to hide it from users; use a module-level `_` prefix instead.

### 2.7 Comprehensions & Generator Expressions

Okay for simple cases. Multiple `for` clauses or filter expressions are not permitted. Optimize for readability.

```python
# Yes
result = [mapping_expr for value in iterable if filter_expr]

# No
result = [(x, y) for x in range(10) for y in range(5) if x * y > 10]

```

### 2.8 Default Iterators and Operators

Use default iterators and operators for types that support them (lists, dicts, files).

```python
# Yes
for key in adict: ...
if obj in alist: ...

```

### 2.9 Generators

Use generators as needed. Use "Yields:" instead of "Returns:" in the docstring.

### 2.10 Lambda Functions

Okay for one-liners. If the code spans multiple lines or is longer than 60-80 chars, define it as a regular nested function.

### 2.11 Conditional Expressions

Okay for simple cases. Each portion must fit on one line: `x = 1 if cond else 2`.

### 2.12 Default Argument Values

Do not use mutable objects as default values in function definitions.

```python
# Yes
def foo(a, b=None):
    if b is None:
        b = []

# No
def foo(a, b=[]):
    ...

```

### 2.13 Properties

Properties may be used for trivial computations. They must be cheap and match the expectations of regular attribute access. Use the `@property` decorator.

### 2.14 True/False Evaluations

Use implicit false if possible: `if not users:` instead of `if len(users) == 0:`.

* Always use `if foo is None:` to check for `None`.
* Never compare a boolean to `False` using `==`. Use `if not x:`.

### 2.17 Function and Method Decorators

Use decorators judiciously. Avoid `staticmethod`. Use `classmethod` only for named constructors or class-specific routines.

### 2.18 Threading

Do not rely on the atomicity of built-in types. Use the `queue` module for communication between threads.

---

## 3 Python Style Rules

### 3.1 Semicolons

Do not terminate lines with semicolons or use them to put two statements on the same line.

### 3.2 Line Length

Maximum line length is **80 characters**.

* Avoid backslashes for line continuation.
* Use implicit line joining inside parentheses, brackets, and braces.

### 3.4 Indentation

Indent code blocks with **4 spaces**. Never use tabs.

### 3.5 Blank Lines

Two blank lines between top-level definitions. One blank line between method definitions.

### 3.6 Whitespace

* No whitespace inside parentheses, brackets, or braces.
* No whitespace before commas, semicolons, or colons.
* Surround binary operators with a single space.
* No spaces around `=` for keyword arguments unless a type annotation is present.

### 3.8 Comments and Docstrings

Use the `"""` format for docstrings.

* **Modules:** Describe contents and usage.
* **Functions:** Mandatory for public APIs, non-trivial size, or non-obvious logic. Include `Args:`, `Returns:`, and `Raises:`.
* **Classes:** Include an `Attributes:` section for public attributes.

### 3.10 Strings

Use f-strings, `%`, or `.format()` for formatting. Avoid `+` and `+=` in loops; use `''.join()` instead.

### 3.12 TODO Comments

Format: `# TODO: reference - explanatory string`.

### 3.13 Imports Formatting

Imports should be on separate lines at the top of the file, grouped:

1. Future imports
2. Standard library
3. Third-party libraries
4. Local sub-packages

### 3.16 Naming

* `module_name`, `package_name`, `ClassName`, `method_name`, `function_name`, `GLOBAL_CONSTANT_NAME`.
* Avoid single-character names except for iterators or exceptions.
* Internal names should start with a single `_`.
