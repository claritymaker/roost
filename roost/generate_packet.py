from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping, Dict


@dataclass
class Definition:
    name_pattern: str
    source: Callable
    apids: Iterable[str]
    args: Iterable[object] = tuple()
    kwargs: Mapping[str, object] = field(default_factory=dict)


@dataclass
class TelemetryPoint:
    name: str
    value: object
    apid: str

def generate_packet(sources: Iterable[Definition], pattern_sources: Mapping[str, str]) -> Iterable[Definition]:
    output =
    for s in sources:
        output = s.source(*s.args, **s.kwargs)

def map_names(pattern, values, pattern_sources) -> Dict[str, object]:
    ...

