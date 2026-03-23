from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass
class PathConfig:
    data_root: Path = Path("/data")
    bronze: Path = Path("/data/bronze")
    silver: Path = Path("/data/silver")
    gold: Path = Path("/data/gold")
    reference: Path = Path("/data/reference")

    def __post_init__(self):
        for path in [self.bronze, self.silver, self.gold, self.reference]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class ModalConfig:
    memory_mb: Dict[str, int] = None
    timeout_seconds: Dict[str, int] = None

    def __post_init__(self):
        if not self.memory_mb:
            self.memory_mb = {
                "extract": 4096,
                "transform": 2048,
                "metrics": 1024,
            }
        if not self.timeout_seconds:
            self.timeout_seconds = {
                "extract": 600,
                "bootstrap": 1200,
                "pipeline": 3600,
            }


@dataclass
class AppSettings:
    paths: PathConfig = None
    modal: ModalConfig = None

    def __post_init__(self):
        self.paths = self.paths or PathConfig()
        self.modal = self.modal or ModalConfig()
