from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Channel:
    name: str
    symbols: List[str]

    def __post_init__(self):
        self.symbols.sort()
