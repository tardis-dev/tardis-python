from dataclasses import dataclass
from typing import Optional, Sequence, Tuple


@dataclass(frozen=True)
class Channel:
    name: str
    symbols: Optional[Sequence[str]] = None

    def __post_init__(self):
        if self.symbols is None:
            return

        normalized_symbols: Tuple[str, ...] = tuple(sorted(self.symbols))
        object.__setattr__(self, "symbols", normalized_symbols)
