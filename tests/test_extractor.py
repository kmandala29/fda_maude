"""
Unit tests for MAUDEExtractor — fully mocked, no real API calls.
Run with: pytest tests/
"""
import pytest
from unittest.mock import patch, MagicMock

from maude_ingestion.extractor import MAUDEExtractor


@pytest.fixture
def extractor():
    return MAUDEExtractor(
        manufacturer="becton dickinson",
        date_from="2023-01-01",
        date_to="2025-12-31",
        page_limit=2,
    )


def _mock_response(results: list, total: int, status=200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {
        "meta": {"results": {"total": total}},
        "results": results,
    }
    resp.raise_for_status = MagicMock()
    return resp


class TestMAUDEExtractor:

    def test_count_total(self, extractor):
        with patch.object(extractor.session, "get", return_value=_mock_response([], total=42)):
            assert extractor.count_total() == 42

    def test_single_page_extraction(self, extractor):
        records = [{"report_number": "A1"}, {"report_number": "A2"}]
        responses = [
            _mock_response(records, total=2),
            _mock_response([], total=2),   # empty page stops iteration
        ]
        with patch.object(extractor.session, "get", side_effect=responses):
            pages = list(extractor.extract())

        assert len(pages) == 1
        assert len(pages[0]) == 2
        assert pages[0][0]["report_number"] == "A1"
        assert "_ingested_at" in pages[0][0]
        assert "_manufacturer_filter" in pages[0][0]

    def test_pagination(self, extractor):
        page1 = [{"report_number": "R1"}, {"report_number": "R2"}]
        page2 = [{"report_number": "R3"}]
        responses = [
            _mock_response([{"report_number": "X"}], total=3),  # count call
            _mock_response(page1, total=3),
            _mock_response(page2, total=3),
            _mock_response([], total=3),
        ]
        with patch.object(extractor.session, "get", side_effect=responses):
            pages = list(extractor.extract())

        assert sum(len(p) for p in pages) == 3

    def test_no_results_returns_empty(self, extractor):
        no_match = MagicMock()
        no_match.status_code = 404
        no_match.json.return_value = {"error": {"message": "No matches found!"}}
        with patch.object(extractor.session, "get", return_value=no_match):
            assert extractor.count_total() == 0

    def test_search_string_format(self, extractor):
        search = extractor._build_search()
        assert "becton dickinson" in search
        assert "20230101" in search
        assert "20251231" in search
