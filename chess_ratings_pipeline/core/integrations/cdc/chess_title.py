from enum import Enum


class ChessTitle(Enum):
    """
    Enumeration representing chess titles.

    Attributes:
        GM: Grandmaster
        WGM: Woman Grandmaster
        IM: International Master
        WIM: Woman International Master
        FM: FIDE Master
        WFM: Woman FIDE Master
        NM: National Master
        WNM: Woman National Master
        CM: Candidate Master
        WCM: Woman Candidate Master
    """

    GM = "Grandmaster"
    WGM = "Woman Grandmaster"
    IM = "International Master"
    WIM = "Woman International Master"
    FM = "FIDE Master"
    WFM = "Woman FIDE Master"
    NM = "National Master"
    WNM = "Woman National Master"
    CM = "Candidate Master"
    WCM = "Woman Candidate Master"
