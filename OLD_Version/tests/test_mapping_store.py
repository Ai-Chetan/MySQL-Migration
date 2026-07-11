"""
tests/test_mapping_store.py
----------------------------
Unit tests for core/mapping_store.py.
Run with: python -m pytest tests/
"""
from __future__ import annotations

from pathlib import Path

import pytest

from core.mapping_store import MappingStore
from models.mapping import SingleMapping, SplitMapping, SplitTarget


@pytest.fixture
def tmp_store(tmp_path: Path) -> MappingStore:
    """Returns a fresh, empty MappingStore backed by a temp file."""
    return MappingStore(tmp_path / "mappings.json")


class TestMappingStoreBasics:
    def test_initially_empty(self, tmp_store: MappingStore) -> None:
        assert tmp_store.all() == {}

    def test_set_and_get_single(self, tmp_store: MappingStore) -> None:
        tmp_store.set_single("users", "users_new")
        m = tmp_store.get("users")
        assert isinstance(m, SingleMapping)
        assert m.target_schema_name == "users_new"

    def test_remove_mapping(self, tmp_store: MappingStore) -> None:
        tmp_store.set_single("orders", "orders_new")
        removed = tmp_store.remove("orders")
        assert removed
        assert tmp_store.get("orders") is None

    def test_remove_nonexistent_returns_false(self, tmp_store: MappingStore) -> None:
        assert not tmp_store.remove("ghost_table")

    def test_all_single(self, tmp_store: MappingStore) -> None:
        tmp_store.set_single("a", "a_new")
        tmp_store.set_single("b", "b_new")
        singles = tmp_store.all_singles()
        assert all(isinstance(v, SingleMapping) for v in singles.values())
        assert len(singles) == 2


class TestMappingStorePersistence:
    def test_save_and_reload(self, tmp_path: Path) -> None:
        path = tmp_path / "mappings.json"
        store1 = MappingStore(path)
        store1.set_single("tbl", "tbl_new")
        store1.save()

        store2 = MappingStore(path)
        store2.load()
        m = store2.get("tbl")
        assert isinstance(m, SingleMapping)
        assert m.target_schema_name == "tbl_new"

    def test_file_created_on_save(self, tmp_path: Path) -> None:
        path = tmp_path / "sub" / "mappings.json"
        store = MappingStore(path)
        store.set_single("t", "t_new")
        store.save()
        assert path.exists()


class TestMappingStoreSplitMaps:
    def test_set_split(self, tmp_store: MappingStore) -> None:
        mapping = SplitMapping(
            source_table="fat_table",
            targets=[
                SplitTarget(schema_name="part_a", column_mappings={}),
                SplitTarget(schema_name="part_b", column_mappings={}),
            ],
        )
        tmp_store.set_mapping("fat_table", mapping)
        m = tmp_store.get("fat_table")
        assert isinstance(m, SplitMapping)
        assert m.target_schema_names == ["part_a", "part_b"]

    def test_all_splits(self, tmp_store: MappingStore) -> None:
        mapping = SplitMapping(
            source_table="src",
            targets=[
                SplitTarget("x", {}),
                SplitTarget("y", {}),
            ],
        )
        tmp_store.set_mapping("src", mapping)
        splits = tmp_store.all_splits()
        assert "src" in splits


class TestColumnMappings:
    def test_set_column_mapping_single(self, tmp_store: MappingStore) -> None:
        tmp_store.set_single("tbl", "tbl_new")
        tmp_store.set_column_mapping("tbl", old_col="first_name", new_col="fname")
        m = tmp_store.get("tbl")
        assert isinstance(m, SingleMapping)
        assert m.column_mappings.get("first_name") == "fname"

    def test_remove_column_mapping_single(self, tmp_store: MappingStore) -> None:
        tmp_store.set_single("tbl", "tbl_new")
        tmp_store.set_column_mapping("tbl", old_col="old", new_col="new")
        tmp_store.remove_column_mapping("tbl", new_col="new")
        m = tmp_store.get("tbl")
        assert isinstance(m, SingleMapping)
        assert "old" not in m.column_mappings

    def test_set_column_mapping_split_target(self, tmp_store: MappingStore) -> None:
        mapping = SplitMapping(
            source_table="src",
            targets=[SplitTarget("a_new", {}), SplitTarget("b_new", {})],
        )
        tmp_store.set_mapping("src", mapping)
        tmp_store.set_column_mapping("src", old_col="x", new_col="y", split_target="a_new")
        m = tmp_store.get("src")
        assert isinstance(m, SplitMapping)
        col_maps = m.column_mappings_for("a_new")
        assert col_maps.get("x") == "y"


class TestAllMappedTargets:
    def test_excludes_current_key(self, tmp_store: MappingStore) -> None:
        tmp_store.set_single("a", "target_x")
        tmp_store.set_single("b", "target_y")
        targets = tmp_store.all_mapped_targets(exclude_key="a")
        assert "target_y" in targets
        assert "target_x" not in targets
