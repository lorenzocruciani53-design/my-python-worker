from __future__ import annotations

from dataclasses import dataclass, field

import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")


@dataclass
class Listing:
    site: str
    id: str
    title: str
    price: float                     # prezzo dell'oggetto
    url: str
    shipping: float | None = None    # spedizione verso di te (None = usa il valore di default del sito)
    buyer_fee: float | None = None   # commissioni/protezione acquisti
    description: str = ""
    image: str = ""
    location: str = ""
    created: str = ""
    seller_name: str = ""
    seller_id: str = ""
    seller_rating: float | None = None   # 0-1 (es. 0.98 = 98% positivi)
    seller_reviews: int | None = None    # numero recensioni
    extra: dict = field(default_factory=dict)

    @property
    def uid(self) -> str:
        return f"{self.site}:{self.id}"


class Source:
    """Interfaccia comune: ogni sito implementa search(query) -> list[Listing]."""
    name = "base"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.http = requests.Session()
        self.http.headers.update({"User-Agent": UA, "Accept-Language": "it-IT,it;q=0.9"})

    def search(self, query: str, max_price: float | None = None) -> list[Listing]:
        raise NotImplementedError

    def enrich_seller(self, item: Listing) -> None:
        """Facoltativo: scarica le recensioni del venditore solo per gli annunci interessanti."""
        return None

    def default_costs(self, price: float) -> tuple[float, float]:
        """(spedizione, commissione acquirente) se l'annuncio non le indica."""
        ship = float(self.cfg.get("spedizione_default", 0))
        fee = float(self.cfg.get("commissione_fissa", 0)) + price * float(self.cfg.get("commissione_perc", 0))
        return ship, round(fee, 2)
