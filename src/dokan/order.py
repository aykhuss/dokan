"""NNLOJET order settings

define an Enum type for the setting of perturbative orders and inquiries
"""

from enum import IntEnum, unique


@unique
class Order(IntEnum):
    """Encode the perturbatibe order"""

    LO = 0
    NLO = 1
    NLO_ONLY = -1  # "ONLY" <-> coefficient
    NNLO = 2
    NNLO_ONLY = -2
    # N3LO = 3
    # N3LO_ONLY = -3

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
        """parse a `part` string into an order

        Parameters
        ----------
        s : str
            string for a `part` of the calculation

        Returns
        -------
        [type]
            `Order` type associated with the `part`

        Raises
        ------
        ValueError
            case parsing failed
        """
        if s.upper() == "LO":
            return Order.LO
        if s.upper() == "NLO":
            return Order.NLO
        if s.upper() in ["NLO_ONLY", "DNLO", "V", "R"]:
            return Order.NLO_ONLY
        if s.upper() == "NNLO":
            return Order.NNLO
        if s.upper() in ["NNLO_ONLY", "DNNLO", "VV", "RV", "RR", "RRA", "RRB"]:
            return Order.NNLO_ONLY
        if s.upper() == "N3LO":
            return Order.N3LO
        if s.upper() in ["N3LO_ONLY", "DN3LO"]:
            return Order.N3LO_ONLY
        raise ValueError(f"Order::partparse: unknown part: {s}")

    def is_in(self, other) -> bool:
        """check if an `Order` is part of another `Order`

        Parameters
        ----------
        other : [type]
            the potential "container" to test the order against

        Returns
        -------
        bool
            True is `self` is contained in `other`
        """
        if other.value < 0:
            # > exact matches for coefficients
            return self.value == other.value
        else:  # other.value >= 0:
            # > NNLO = LO + NLO_ONLY + NNLO_ONLY
            return abs(self.value) <= other.value
