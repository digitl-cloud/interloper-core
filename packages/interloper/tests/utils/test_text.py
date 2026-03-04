"""Tests for text utilities."""

import pytest

from interloper.utils.text import slugify, to_label, to_snake_case, validate_name


class TestValidateName:
    """Tests for validate_name()."""

    def test_simple_lowercase(self):
        validate_name("asset")  # should not raise

    def test_with_underscores(self):
        validate_name("my_asset")

    def test_with_numbers(self):
        validate_name("source1")

    def test_pascal_case(self):
        validate_name("MySource")

    def test_mixed(self):
        validate_name("Facebook_Ads_v2")

    def test_single_letter(self):
        validate_name("a")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_name("")

    def test_rejects_starts_with_number(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_name("1asset")

    def test_rejects_starts_with_underscore(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_name("_private")

    def test_rejects_hyphens(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_name("my-asset")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_name("my asset")

    def test_rejects_dots(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_name("my.asset")

    def test_rejects_special_characters(self):
        with pytest.raises(ValueError, match="invalid"):
            validate_name("asset@home")


class TestSlugify:
    """Tests for slugify()."""

    def test_empty_string(self):
        assert slugify("") == ""

    def test_single_word(self):
        assert slugify("asset") == "asset"

    def test_underscores_to_hyphens(self):
        assert slugify("my_asset") == "my-asset"

    def test_multiple_underscores_collapsed(self):
        assert slugify("my__asset") == "my-asset"

    def test_already_hyphenated(self):
        assert slugify("already-slugged") == "already-slugged"

    def test_camel_case(self):
        assert slugify("myAsset") == "my-asset"

    def test_pascal_case(self):
        assert slugify("MyAsset") == "my-asset"

    def test_mixed_case_and_underscores(self):
        assert slugify("my_Asset_Name") == "my-asset-name"

    def test_leading_trailing_underscores_stripped(self):
        assert slugify("_my_asset_") == "my-asset"

    def test_leading_trailing_hyphens_stripped(self):
        assert slugify("-my-asset-") == "my-asset"

    def test_spaces_to_hyphens(self):
        assert slugify("my asset") == "my-asset"

    def test_mixed_separators(self):
        assert slugify("my_asset-name") == "my-asset-name"

    def test_uppercase_lowered(self):
        assert slugify("MY_ASSET") == "my-asset"

    def test_numbers_preserved(self):
        assert slugify("source1") == "source1"

    def test_numbers_with_underscores(self):
        assert slugify("source_1") == "source-1"

    def test_camel_with_numbers(self):
        assert slugify("myAsset2Name") == "my-asset2-name"

    def test_real_world_asset_names(self):
        assert slugify("facebook_ads") == "facebook-ads"
        assert slugify("campaign_performance_analysis") == "campaign-performance-analysis"
        assert slugify("asset_a") == "asset-a"
        assert slugify("upstream_asset") == "upstream-asset"

    def test_idempotent(self):
        """Slugifying an already-slugified string returns the same result."""
        original = "my-asset-name"
        assert slugify(original) == original
        assert slugify(slugify("my_Asset_Name")) == "my-asset-name"


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
