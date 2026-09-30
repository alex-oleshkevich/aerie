"""TextChoices: a StrEnum whose members carry a label."""

from aerie.choices import TextChoices


class Colour(TextChoices):
    RED = "red", "Rouge"
    DEEP_BLUE = "deep_blue"  # label derived from the value


class TestLabels:
    def test_explicit_label_is_kept(self) -> None:
        assert Colour.RED.label == "Rouge"

    def test_label_is_derived_when_omitted(self) -> None:
        assert Colour.DEEP_BLUE.label == "Deep blue"

    def test_members_are_still_strings(self) -> None:
        assert Colour.RED.value == "red"
        assert f"{Colour.DEEP_BLUE}" == "deep_blue"


class TestHelpers:
    def test_items_maps_members_to_labels(self) -> None:
        assert Colour.items() == {Colour.RED: "Rouge", Colour.DEEP_BLUE: "Deep blue"}

    def test_label_for(self) -> None:
        assert Colour.label_for(Colour.RED) == "Rouge"
