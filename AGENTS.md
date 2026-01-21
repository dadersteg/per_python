# Instructions

## 1. Version Control & Header
Every script file MUST begin with a prominent version header formatted exactly as shown below:

'''
# ============================================================================
# [PROJECT NAME] v[VERSION NUMBER]
# ============================================================================
'''

* Mandatory Inclusion: Jules must ALWAYS add this label at the very top of every file.
* Mandatory Update: Jules must ALWAYS increment the version number whenever the logic is modified or optimized.

## 2. Executive Summary Standard (BLUF)
Every script file MUST include a top-level comment block titled "EXECUTIVE SUMMARY (BLUF)" immediately following the version header.

* Goal: Explain what the code does for a non-technical stakeholder.
* Plain English Section: Include a summary sentence explaining the main benefit in clear, non-technical language.
* Business Logic: Briefly state which business problem this specific file solves.

## 3. Documentation Standards
* Detailed Docstrings: Every function must have Google-style docstrings that explain both the 'what' and the 'why'.
* Clarifying Inline Comments: Use frequent inline comments to explain logic steps in plain English.
* Step-by-Step: Break complex logic into numbered steps (e.g., "# Step 1: Initialize").

## 4. Coding Standards
* Runtime: Always use Python 3.12+.
* Naming: Use clear, descriptive snake_case variable names.
* Logging: Use the standard 'logging' library for transparency.

## 5. Operational Boundaries
* Do not add external libraries without explicit approval.
* Always ask before deleting or overwriting substantial existing logic.