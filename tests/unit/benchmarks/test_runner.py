"""Tests for the benchmarks runner module."""
import pytest
from unittest.mock import patch, MagicMock
from genie_world.benchmarks.runner import run_questions, extract_questions_from_config
from genie_world.benchmarks.models import QuestionInput, QuestionSource
from genie_world.core.genie_client import GenieResponse


class TestExtractQuestionsFromConfig:
    def test_extracts_from_benchmarks(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["What is total revenue?"],
                        "answer": [{"format": "SQL", "content": ["SELECT SUM(amount)", "FROM orders"]}],
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert len(questions) == 1
        assert questions[0].question == "What is total revenue?"
        assert questions[0].expected_sql == "SELECT SUM(amount) FROM orders"
        assert questions[0].source == QuestionSource.SPACE_CONFIG

    def test_empty_benchmarks(self):
        config = {"benchmarks": {"questions": []}}
        assert extract_questions_from_config(config) == []

    def test_no_benchmarks_section(self):
        config = {"data_sources": {"tables": []}}
        assert extract_questions_from_config(config) == []

    def test_skips_parameterized_without_defaults(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["Revenue for :region"],
                        "answer": [{"format": "SQL", "content": ["SELECT SUM(amount) FROM orders WHERE region = :region"]}],
                        "parameters": [{"name": "region", "type_hint": "STRING"}],
                        # No default_value → skipped
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert len(questions) == 0

    def test_substitutes_parameterized_with_defaults(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["Revenue for :region"],
                        "answer": [{"format": "SQL", "content": ["SELECT SUM(amount) FROM orders WHERE region = :region"]}],
                        "parameters": [{"name": "region", "type_hint": "STRING", "default_value": "WEST"}],
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert len(questions) == 1
        assert "WEST" in questions[0].question
        assert "WEST" in questions[0].expected_sql

    def test_multiple_questions(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["Q1"],
                        "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                    },
                    {
                        "question": ["Q2"],
                        "answer": [{"format": "SQL", "content": ["SELECT 2"]}],
                    },
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert len(questions) == 2
        assert questions[0].question == "Q1"
        assert questions[1].question == "Q2"

    def test_multiword_question_joined(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["What", "is", "the", "total?"],
                        "answer": [{"format": "SQL", "content": ["SELECT SUM(x) FROM t"]}],
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert questions[0].question == "What is the total?"

    def test_multiline_sql_joined(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["How many?"],
                        "answer": [{"format": "SQL", "content": ["SELECT COUNT(*)", "FROM orders", "WHERE status = 'active'"]}],
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert questions[0].expected_sql == "SELECT COUNT(*) FROM orders WHERE status = 'active'"

    def test_missing_answer_skipped(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["Q?"],
                        "answer": [],
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert len(questions) == 0


class TestRunQuestions:
    @patch("genie_world.benchmarks.runner.GenieClient")
    def test_runs_questions_in_parallel(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.ask.return_value = GenieResponse(
            question="test", status="COMPLETED",
            generated_sql="SELECT 1", duration_seconds=1.0,
        )

        questions = [
            QuestionInput(question="Q1", expected_sql="SELECT 1", source=QuestionSource.CUSTOM),
            QuestionInput(question="Q2", expected_sql="SELECT 2", source=QuestionSource.CUSTOM),
        ]

        responses = run_questions("space-1", questions, max_workers=2)
        assert len(responses) == 2
        assert mock_client.ask.call_count == 2

    @patch("genie_world.benchmarks.runner.GenieClient")
    def test_returns_genie_responses(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.ask.side_effect = [
            GenieResponse(question="Q1", status="COMPLETED", generated_sql="SELECT 1", duration_seconds=0.5),
            GenieResponse(question="Q2", status="FAILED", error="timeout", duration_seconds=10.0),
        ]

        questions = [
            QuestionInput(question="Q1", expected_sql="SELECT 1", source=QuestionSource.SPACE_CONFIG),
            QuestionInput(question="Q2", expected_sql="SELECT 2", source=QuestionSource.SPACE_CONFIG),
        ]

        responses = run_questions("space-1", questions, max_workers=2)
        assert len(responses) == 2
        statuses = {r.status for r in responses}
        assert "COMPLETED" in statuses
        assert "FAILED" in statuses

    @patch("genie_world.benchmarks.runner.GenieClient")
    def test_empty_questions(self, mock_client_class):
        responses = run_questions("space-1", [], max_workers=2)
        assert responses == []
        mock_client_class.assert_not_called()

    @patch("genie_world.benchmarks.runner.GenieClient")
    def test_creates_client_with_space_id(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.ask.return_value = GenieResponse(
            question="Q", status="COMPLETED", duration_seconds=1.0
        )

        questions = [QuestionInput(question="Q", expected_sql="SELECT 1", source=QuestionSource.CUSTOM)]
        run_questions("my-space-id", questions)

        mock_client_class.assert_called_once_with("my-space-id")
