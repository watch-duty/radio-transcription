---
applyTo: "**/*.ts, **/*.tsx, **/*.js, **/*.jsx"
---

Below is a style guide for the Python code in this project. Please follow these guidelines when contributing and reviewing code.

# TS/JS Style Guide

## Introduction

This is the style guide for the TypeScript and JavaScript languages, including TSX. A TypeScript or JavaScript source file is described as being *in Style* if and only if it adheres to the rules herein. 

This style guide is TypeScript first. All rules apply to both TypeScript and JavaScript inasmuch as they are applicable to both languages. 

### Terminology notes

This Style Guide uses RFC 2119 terminology when using the phrases *must*, *must not*, *should*, *should not*, and *may*. The terms *prefer* and *avoid* correspond to *should* and *should not*, respectively. Imperative and declarative statements are prescriptive and correspond to *must*.

### Guide notes

All examples given are **non-normative** and serve only to illustrate the normative language of the style guide. Optional formatting choices made in examples must not be enforced as rules.

## Source file basics

### File names

File names use `snake_case`, e.g. `my_fancy_ctrl.ts`, lower case only. The basename *must* match `[a-z0-9_]+`. In particular, dots are not allowed.

#### Test file names

Test files *must* end in `_test.ts` or `_test.tsx` and *should* match the name of the code under test, e.g. `my_fancy_ctrl_test.ts`. Support libraries for tests do not use the `_test.ts` suffix.

### File encoding: UTF-8

Source files are encoded in **UTF-8**.

#### Whitespace characters

Aside from the line terminator sequence, the ASCII horizontal space character (0x20) is the only whitespace character that appears anywhere in a source file. This implies that all other whitespace characters in string literals are escaped.

#### Special escape sequences

For any character that has a special escape sequence (`\'`, `\"`, `\\ `, `\b`, `\f`, `\n`, `\r`, `\t`, `\v`), that sequence is used rather than the corresponding numeric escape (e.g `\x0a`, `\u000a`, or `\u{a}`). Legacy octal escapes are never used.

#### Non-ASCII characters

For the remaining non-ASCII characters, use the actual Unicode character (e.g. `∞`). For non-printable characters, the equivalent hex or Unicode escapes (e.g. `\u221e`) can be used along with an explanatory comment.

```ts
// Perfectly clear, even without a comment.
const units = 'μs';

// Use escapes for non-printable characters.
const output = '\ufeff' + content;  // byte order mark

```

## Source file structure

Files consist of the following, **in order**:

1. Imports, if present
2. The file’s implementation

**Exactly one blank line** separates each section that is present.

### Imports

There are four variants of import statements in ES6 and TypeScript:

| Import type | Example | Use for |
| --- | --- | --- |
| namespace/module | `import * as foo from '...';` | TypeScript and some JavaScript imports |
| named/destructuring | `import {SomeThing} from '...';` | TypeScript and some JavaScript imports |
| default | `import SomeThing from '...';` | Only for external code that requires them |
| side-effect | `import '...';` | Only to import libraries for their side-effects on load |

```ts
// Good: choose between two options as appropriate (see below).
import * as ng from '@angular/core';
import {Foo} from './foo';

// Sometimes needed to import libraries for their side effects:
import 'jasmine';
import '@polymer/paper-button';

// Code may also use aliasing imports when dealing with namespaces:
import Nested = Namespace.Nested;

```

#### Import paths

TypeScript code *must* use paths to import other TypeScript code. Paths *may* be relative, i.e. starting with `.` or `..`, or rooted at the base directory. Code *should* use relative imports (`./foo`) rather than absolute imports when referring to files within the same (logical) project.

Consider limiting the number of parent steps (`../../../`) as those can make module and path structures hard to understand.

#### Namespace versus named imports

Prefer named imports for symbols used frequently in a file or for symbols that have clear names. Named imports can be aliased to clearer names as needed with `as`.

Prefer namespace imports when using many different symbols from large APIs. Namespace imports can aid readability for exported symbols that have common names like `Model` or `Controller` without the need to declare aliases.

#### Renaming imports

Code *should* fix name collisions by using a namespace import or renaming the exports themselves. Code *may* rename imports (`import {SomeThing as SomeOtherThing}`) if needed.

### Exports

Use named exports in all code. Do not use default exports. This ensures that all imports follow a uniform pattern.

```ts
// Use named exports:
export class Foo { ... }

```

```ts
// Do not use default exports:
export default class Foo { ... } // BAD!

```

#### Mutable exports

Regardless of technical support, mutable exports can create hard to understand and debug code. `export let` is not allowed. If one needs to support externally accessible and mutable bindings, they *should* instead use explicit getter functions.

```ts
let foo = 3;
window.setTimeout(() => {
  foo = 4;
}, 1000 /* ms */);
// Use an explicit getter to access the mutable export.
export function getFoo() { return foo; };

```

#### Container classes

Do not create container classes with static methods or properties for the sake of namespacing. Instead, export individual constants and functions:

```ts
// GOOD
export const FOO = 1;
export function bar() { return 1; }

```

### Import and export type

#### Import type

You may use `import type {...}` when you use the imported symbol only as a type. Use regular imports for values.

#### Export type

Use `export type` when re-exporting a type, e.g.:

```ts
export type {AnInterface} from './foo';

```

#### Dynamic `import()` type

Do not use the `import()` function inside type annotations. If you need to ensure that the import is always elided from the generated JavaScript, use `import type` instead.

### Use modules not namespaces

TypeScript supports two methods to organize code: *namespaces* and *modules*, but namespaces are disallowed. Code *must* refer to code in other files using imports and exports of the form `import {foo} from 'bar';`

Your code *must not* use the `namespace Foo { ... }` construct.

## Language features

### Local variable declarations

#### Use const and let

Always use `const` or `let` to declare variables. Use `const` by default, unless a variable needs to be reassigned. Never use `var`. Variables *must not* be used before their declaration.

#### One variable per declaration

Every local variable declaration declares only one variable: declarations such as `let a = 1, b = 2;` are not used.

### Array literals

#### Do not use the `Array` constructor

*Do not* use the `Array()` constructor, with or without `new`. Instead, always use bracket notation to initialize arrays, or `from` to initialize an `Array` with a certain size.

#### Do not define properties on arrays

Do not define or use non-numeric properties on an array (other than `length`). Use a `Map` (or `Object`) instead.

#### Array destructuring

Array literals may be used on the left-hand side of an assignment to perform destructuring. A final "rest" element may be included. Elements should be omitted if they are unused. Always specify `[]` as the default value if a destructured array parameter is optional.

### Object literals

#### Do not use the `Object` constructor

The `Object` constructor is disallowed. Use an object literal (`{}` or `{a: 0, b: 1}`) instead.

#### Iterating objects

Do not use unfiltered `for (... in ...)` statements. Either filter values explicitly with an `if` statement, or use `for (... of Object.keys(...))`.

#### Using spread syntax

Using spread syntax `{...bar}` is a convenient shorthand for creating a shallow copy of an object. Avoid spreading objects that have prototypes other than the Object prototype (e.g. class definitions, class instances, functions).

#### Computed property names

Computed property names (e.g. `{['key' + foo()]: 42}`) are allowed, and are considered dict-style (quoted) keys unless the computed property is a symbol.

#### Object destructuring

Destructured objects may also be used as function parameters, but should be kept as simple as possible: a single level of unquoted shorthand properties. Deeper levels of nesting and computed properties may not be used in parameter destructuring.

### Classes

#### Class declarations

Class declarations *must not* be terminated with semicolons. Statements that contain class expressions *must* be terminated with a semicolon.

#### Don't create member variables only used in constructors

If a variable is only used in the constructor, migrate it to be a variable scoped to the constructor body, which will make your class more stateless.

#### Class method declarations

Class method declarations *must not* use a semicolon to separate individual method declarations. Method declarations should be separated from surrounding code by a single blank line.

#### Static methods

Code *should not* rely on dynamic dispatch of static methods. Code *must not* use `this` in a static context.

#### Constructors

Constructor calls *must* use parentheses, even when no arguments are passed: `new Foo()`.

#### Class members

* **No #private fields**: Do not use private fields (also known as private identifiers). Instead, use TypeScript's visibility annotations (`private`, `protected`).
* **Use readonly**: Mark properties that are never reassigned outside of the constructor with the `readonly` modifier.
* **Parameter properties**: Rather than plumbing an obvious initializer through to a class member, use a TypeScript parameter property (`constructor(private readonly barService: BarService) {}`).

#### Visibility

Limit symbol visibility as much as possible. TypeScript symbols are public by default. Never use the `public` modifier except when declaring non-readonly public parameter properties (in constructors).

### Functions

#### Prefer function declarations for named functions

Prefer function declarations (`function foo() {}`) over arrow functions or function expressions when defining named functions.

#### Do not use function expressions

Do not use function expressions (`function() { ... }`). Use arrow functions instead.

#### Arrow function bodies

Only use a concise body (`() => expression`) if the return value of the function is actually used. Use a block body (`() => { ... }`) if the return type is `void` to prevent potential side effects.

#### Rebinding `this`

Function expressions and function declarations *must not* use `this` unless they specifically exist to rebind the `this` pointer. Prefer arrow functions.

#### Prefer passing arrow functions as callbacks

Avoid passing a named callback to a higher-order function, unless you are sure of the stability of both functions' call signatures. Prefer passing an arrow-function that explicitly forwards parameters to the named callback.

#### Parameter initializers

Optional function parameters *may* be given a default initializer to use when the argument is omitted. Initializers *must not* have any observable side effects.

#### Prefer rest and spread when appropriate

Use a *rest* parameter instead of accessing `arguments`. Never name a local variable or parameter `arguments`. Use function spread syntax instead of `Function.prototype.apply`.

### this

Only use `this` in class constructors and methods, functions that have an explicit `this` type declared, or in arrow functions defined in a scope where `this` may be used. Never use `this` to refer to the global object.

### Primitive literals

* **String literals**: Ordinary string literals are delimited with single quotes (`'`).
* **No line continuations**: Do not use *line continuations* (ending a line inside a string literal with a backslash).
* **Template literals**: Use template literals (```) over complex string concatenation.
* **Number literals**: Use exactly `0x`, `0o`, and `0b` prefixes, with lowercase letters, for hex, octal, and binary.

### Type coercion

Values of enum types *must not* be converted to booleans with `Boolean()` or `!!`, and must instead be compared explicitly with comparison operators.

Code *must not* use unary plus (e.g. `+str`) or `parseFloat` to coerce strings to numbers. Prefer `Number` instead.

### Control structures

Control flow statements always use braced blocks for the containing code, even if the body contains only a single statement.
**Exception:** `if` statements fitting on one line *may* elide the block.

#### Iterating containers

Prefer `for (... of someArr)` to iterate over arrays. `for`-`in` loops may only be used on dict-style objects. Do not use `for (... in ...)` to iterate over arrays.

### Exception handling

* **Instantiate errors using `new**`: Always use `new Error()`.
* **Only throw errors**: Do not throw primitives or objects that are not instances of `Error` or a subclass.
* **Empty catch blocks**: It is very rarely correct to do nothing in response to a caught exception. If appropriate, explain why in a comment.

#### Switch statements

All `switch` statements *must* contain a `default` statement group, even if it contains no code. Non-empty statement groups (`case ...`) *must not* fall through.

#### Equality checks

Always use triple equals (`===`) and not equals (`!==`).
**Exception:** Comparisons to the literal `null` value *may* use the `==` and `!=` operators to cover both `null` and `undefined`.

#### Type and non-nullability assertions

Type assertions (`x as SomeType`) and non-nullability assertions (`y!`) are unsafe and *should not* be used without an obvious or explicit reason.

### Decorators

Do not define new decorators. Only use the decorators defined by frameworks (e.g. Angular).

### Disallowed features

* **Wrapper objects**: *Must not* instantiate the wrapper classes for the primitive types `String`, `Boolean`, and `Number`.
* **Automatic Semicolon Insertion**: Do not rely on ASI. Explicitly end all statements using a semicolon.
* **Const enums**: Code *must not* use `const enum`; use plain `enum` instead.
* **`with` and `eval**`: Do not use `with`, `eval`, or the `Function(...string)` constructor.
* **Modifying built-in objects**: Never modify built-in types.

## Naming

### Identifiers

Identifiers *must* use only ASCII letters, digits, underscores (for constants and test names), and (rarely) the '$' sign.

* Do not use trailing or leading underscores for private properties or methods.
* Names *must* be descriptive and clear. Do not use ambiguous abbreviations.

### Rules by identifier type

| Style | Category |
| --- | --- |
| `UpperCamelCase` | class / interface / type / enum / decorator / type params |
| `lowerCamelCase` | variable / parameter / function / method / property |
| `CONSTANT_CASE` | global constant values, including enum values |

## Type system

### Type inference

Code *may* rely on type inference for trivially inferred types. Leave out explicit generic types where they are redundant with the type annotation.

### Undefined and null

Type aliases *must not* include `|null` or `|undefined` in a union type. Code *should* deal with null values close to where they arise.

### Use structural types

Use interfaces to define structural types, not classes.

### Prefer interfaces over type literal aliases

When declaring types for objects, use interfaces instead of a type alias for the object literal expression.

### `Array<T>` Type

For simple types, use the syntax sugar for arrays, `T[]` or `readonly T[]`. For complex types, use the longer form `Array<T>`.

### `any` Type

Code generally *should not* use `any`. Instead, provide a more specific type, use `unknown`, or suppress the lint warning and document why.

### `{}` Type

Code **should not** use `{}` for most use cases. Prefer `unknown`, `Record<string, T>`, or `object`.

## Comments and documentation

* Use `/** JSDoc */` comments for documentation (users of the code).
* Use `// line comments` for implementation comments.
* JSDoc is written in Markdown. Use Markdown lists instead of plain text indentations.
* Line-wrapped block tags are indented four spaces.
* Do not declare types in `@param` or `@return` blocks if redundant with TypeScript.
* Place documentation prior to decorators, not between the decorator and the statement.
