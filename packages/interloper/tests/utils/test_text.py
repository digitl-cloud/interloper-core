"""Tests for text utilities."""

import pytest

from interloper.utils.text import to_label, to_slug_case, to_snake_case, validate_key


class TestValidateName:
    """Tests for validate_key()."""

    def test_simple_lowercase(self):
        validate_key("asset")  # should not raise

    def test_with_underscores(self):
        validate_key("my_asset")

    def test_with_numbers(self):
        validate_key("source1")

    def test_pascal_case(self):
        validate_key("MySource")

    def test_mixed(self):
        validate_key("Facebook_Ads_v2")

    def test_single_letter(self):
        validate_key("a")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_key("")

    def test_rejects_starts_with_number(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_key("1asset")

    def test_rejects_starts_with_underscore(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_key("_private")

    def test_rejects_hyphens(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_key("my-asset")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_key("my asset")

    def test_rejects_dots(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_key("my.asset")

    def test_rejects_special_characters(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_key("asset@home")


class TestToSlugCase:
    """Tests for to_slug_case()."""

    def test_empty_string(self):
        assert to_slug_case("") == ""

    def test_single_word(self):
        assert to_slug_case("asset") == "asset"

    def test_underscores_to_hyphens(self):
        assert to_slug_case("my_asset") == "my-asset"

    def test_multiple_underscores_collapsed(self):
        assert to_slug_case("my__asset") == "my-asset"

    def test_already_hyphenated(self):
        assert to_slug_case("already-slugged") == "already-slugged"

    def test_camel_case(self):
        assert to_slug_case("myAsset") == "my-asset"

    def test_pascal_case(self):
        assert to_slug_case("MyAsset") == "my-asset"

    def test_mixed_case_and_underscores(self):
        assert to_slug_case("my_Asset_Name") == "my-asset-name"

    def test_leading_trailing_underscores_stripped(self):
        assert to_slug_case("_my_asset_") == "my-asset"

    def test_leading_trailing_hyphens_stripped(self):
        assert to_slug_case("-my-asset-") == "my-asset"

    def test_spaces_to_hyphens(self):
        assert to_slug_case("my asset") == "my-asset"

    def test_mixed_separators(self):
        assert to_slug_case("my_asset-name") == "my-asset-name"

    def test_uppercase_lowered(self):
        assert to_slug_case("MY_ASSET") == "my-asset"

    def test_numbers_preserved(self):
        assert to_slug_case("source1") == "source1"

    def test_numbers_with_underscores(self):
        assert to_slug_case("source_1") == "source-1"

    def test_camel_with_numbers(self):
        assert to_slug_case("myAsset2Name") == "my-asset2-name"

    def test_real_world_asset_names(self):
        assert to_slug_case("facebook_ads") == "facebook-ads"
        assert to_slug_case("campaign_performance_analysis") == "campaign-performance-analysis"
        assert to_slug_case("asset_a") == "asset-a"
        assert to_slug_case("upstream_asset") == "upstream-asset"

    def test_idempotent(self):
        """Slugifying an already-slugified string returns the same result."""
        original = "my-asset-name"
        assert to_slug_case(original) == original
        assert to_slug_case(to_slug_case("my_Asset_Name")) == "my-asset-name"


class TestToLabel:
    """Tests for to_label()."""

    def test_empty_string(self):
        assert to_label("") == ""

    def test_underscore_separated(self):
        assert to_label("my_asset") == "My Asset"

    def test_hyphen_separated(self):
        assert to_label("my-asset") == "My Asset"

    def test_camel_case(self):
        assert to_label("myAsset") == "My Asset"


class TestToSnakeCase:
    """Tests for to_snake_case()."""

    def test_empty_string(self):
        assert to_snake_case("") == ""

    def test_camel_case(self):
        assert to_snake_case("userName") == "user_name"

    def test_pascal_case(self):
        assert to_snake_case("UserName") == "user_name"

    def test_already_snake(self):
        assert to_snake_case("user_name") == "user_name"

    def test_hyphens(self):
        assert to_snake_case("user-name") == "user_name"

    def test_spaces(self):
        assert to_snake_case("user name") == "user_name"

    def test_acronym(self):
        assert to_snake_case("XMLParser") == "xml_parser"

    def test_numbers_preserved(self):
        assert to_snake_case("value1Name") == "value1_name"

    def test_idempotent(self):
        assert to_snake_case("already_snake") == "already_snake"
        assert to_snake_case(to_snake_case("UserName")) == "user_name"

    def test_special_characters(self):
        assert to_snake_case("cost%") == "cost"
        assert to_snake_case("hello@world") == "hello_world"

    def test_mixed_separators(self):
        assert to_snake_case("my-Asset_Name") == "my_asset_name"

    def test_all_caps(self):
        assert to_snake_case("HTTP") == "http"

    def test_all_caps_with_suffix(self):
        assert to_snake_case("HTTPResponse") == "http_response"
