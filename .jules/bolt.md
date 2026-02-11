# Bolt's Journal

## 2024-05-22 - Missing Dependencies in Environment
**Learning:** The execution environment lacks internet access and pre-installed data science packages (pandas, numpy, rapidfuzz).
**Action:** Use `unittest.mock` to mock these dependencies when verifying script imports and structure. Trust standard pandas best practices (vectorization) for performance improvements where local benchmarking is impossible.
