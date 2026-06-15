from typing import List, Optional
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

class _FixedFilePartition(StatefulSourcePartition):
    def __init__(self, path: str, resume_state: Optional[int]):
        self._f = open(path, "r")
        if resume_state is not None:
            self._f.seek(resume_state)
            
    def next_batch(self, _sched: Optional[float] = None) -> List[str]:
        lines = self._f.readlines(1000)
        if not lines:
            raise StopIteration()
        return lines
        
    def snapshot(self) -> int:
        return self._f.tell()
        
    def close(self):
        self._f.close()

class FixedFileSource(FixedPartitionedSource):
    def __init__(self, path: str):
        self._path = path
        
    def list_parts(self) -> List[str]:
        return ["fixed_input_part"]
        
    def build_part(self, step_id: str, for_part: str, resume_state: Optional[int]) -> _FixedFilePartition:
        return _FixedFilePartition(self._path, resume_state)
