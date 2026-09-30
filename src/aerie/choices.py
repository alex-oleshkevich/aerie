import enum
import typing


class TextChoices(enum.StrEnum):
    """StrEnum variant with labelled members."""

    label: str

    def __new__(cls, value: str, label: str | None = None) -> typing.Self:
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.label = label or value.replace("_", " ").capitalize()
        return obj

    @classmethod
    def items(cls) -> dict[typing.Self, str]:
        return {member: str(member.label) for member in cls}

    @classmethod
    def label_for(cls, value: typing.Self) -> str:
        return value.label
