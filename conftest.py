"""
Root conftest.py — pytest path resolution.

Ensures the project root is on sys.path so tests can import from src
without requiring `pip install -e .`.
This file intentionally contains no fixtures.
"""
