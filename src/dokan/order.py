from enum import IntEnum, unique


@unique
class Order(IntEnum):
    """Encode the perturbatibe order"""

    LO = 0
    NLO = 1
    NLO_ONLY = -1  # coefficient
    NNLO = 2
    NNLO_ONLY = -2
    N3LO = 3
    N3LO_ONLY = -3

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @staticmethod
    def argparse(s: str):
        """method for `argparse`"""
        try:
            return Order[s.upper()]
        except KeyError:
            return s

    @staticmethod
    def partparse(s: str):
        if s.upper() == "LO":
            return Order.LO
        if s.upper() == "NLO":
            return Order.NLO
        if s.upper() in ["NLO_ONLY", "V", "R"]:
            return Order.NLO_ONLY
        if s.upper() == "NNLO":
            return Order.NNLO
        if s.upper() in ["NNLO_ONLY", "VV", "RV", "RR", "RRA", "RRB"]:
            return Order.NNLO_ONLY
        if s.upper() == "N3LO":
            return Order.N3LO
        if s.upper() in ["N3LO_ONLY"]:
            return Order.N3LO_ONLY
        raise ValueError(f"Order.partparse: unknown part: {s}")
