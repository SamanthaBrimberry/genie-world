import pytest
from genie_world.builder.assembler import assemble_space


class TestAssembleSpace:
    def test_basic_structure(self):
        config = assemble_space(
            data_sources={"tables": [{"identifier": "main.s.t", "column_configs": []}]},
            join_specs=[],
            instructions=[{"content": ["Do this"]}],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[{"question": "How many?", "sql": "SELECT COUNT(*) FROM t"}],
        )

        assert config["version"] == 2
        assert "config" in config
        assert "data_sources" in config
        assert "instructions" in config
        assert config["instructions"]["text_instructions"][0]["content"] == ["Do this"]
        assert len(config["instructions"]["example_question_sqls"]) == 1

    def test_generates_32_char_hex_ids(self):
        config = assemble_space(
            data_sources={"tables": []},
            join_specs=[{"left": {}, "right": {}, "sql": ["a = b"]}],
            instructions=[{"content": ["Test"]}],
            snippets={"filters": [{"sql": "x = 1"}], "expressions": [], "measures": []},
            examples=[],
        )

        # Join specs are stored in _generated_join_specs (not deployed due to API issue)
        js = config["_generated_join_specs"][0]
        assert "id" in js
        assert len(js["id"]) == 32
        assert js["id"].isalnum()
        # Deployed join_specs should be empty
        assert config["instructions"]["join_specs"] == []

    def test_derives_sample_questions(self):
        config = assemble_space(
            data_sources={"tables": []},
            join_specs=[],
            instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[
                {"question": "Q1", "sql": "SELECT 1"},
                {"question": "Q2", "sql": "SELECT 2"},
                {"question": "Q3", "sql": "SELECT 3"},
                {"question": "Q4", "sql": "SELECT 4"},
                {"question": "Q5", "sql": "SELECT 5"},
            ],
        )

        sample_qs = config["config"]["sample_questions"]
        assert 3 <= len(sample_qs) <= 5
        for sq in sample_qs:
            assert "id" in sq
            assert "question" in sq
            assert isinstance(sq["question"], list)
            # No SQL in sample_questions
            assert "sql" not in sq

    def test_wraps_strings_in_lists(self):
        config = assemble_space(
            data_sources={"tables": []},
            join_specs=[],
            instructions=[{"content": "bare string"}],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[{"question": "Q", "sql": "SELECT 1"}],
        )

        assert config["instructions"]["text_instructions"][0]["content"] == ["bare string"]

    def test_benchmarks_optional(self):
        config = assemble_space(
            data_sources={"tables": []}, join_specs=[], instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[],
        )
        assert "benchmarks" not in config or config["benchmarks"] is None

    def test_includes_benchmarks_when_provided(self):
        config = assemble_space(
            data_sources={"tables": []}, join_specs=[], instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[],
            benchmarks={"questions": [{"question": "Q", "answer": [{"format": "SQL", "content": ["SELECT 1"]}]}]},
        )
        assert len(config["benchmarks"]["questions"]) == 1
        assert "id" in config["benchmarks"]["questions"][0]

    def test_benchmark_question_strings_wrapped_in_arrays(self):
        """Bare string 'question' fields in benchmarks must be wrapped as lists."""
        config = assemble_space(
            data_sources={"tables": []}, join_specs=[], instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[],
            benchmarks={"questions": [
                {"question": "How many orders?", "answer": [{"format": "SQL", "content": ["SELECT COUNT(*) FROM t"]}]},
            ]},
        )
        q = config["benchmarks"]["questions"][0]
        assert isinstance(q["question"], list), f"Expected list, got {type(q['question'])}"
        assert q["question"] == ["How many orders?"]

    def test_passthrough_sql_functions(self):
        config = assemble_space(
            data_sources={"tables": []}, join_specs=[], instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[],
            sql_functions=[{"identifier": "main.s.my_func"}],
        )
        funcs = config["instructions"]["sql_functions"]
        assert len(funcs) == 1
        assert "id" in funcs[0]
