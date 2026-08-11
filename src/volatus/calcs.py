class Avg:
    def __init__(self, count: int):
        self._count = count
        self._vals: list[float] = []
        self._avg: float = 0.0

    def add(self, val: float) -> float:
        val = val / self._count
        self._vals.append(val)
        self._avg += val

        if len(self._vals) > self._count:
            self._avg -= self._vals.pop(0)

        return self._avg
