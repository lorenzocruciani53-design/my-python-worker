"""eBay Italia tramite l'API ufficiale (Browse API).

Serve un'app gratuita su https://developer.ebay.com (Client ID + Client Secret,
"Production keys"). Variabili d'ambiente: EBAY_CLIENT_ID, EBAY_CLIENT_SECRET.
Le recensioni del venditore (percentuale e numero feedback) arrivano già nei risultati.
"""
from __future__ import annotations

import base64
import os
import time

from .base import Listing, Source

API = "https://api.ebay.com"


class Ebay(Source):
    name = "ebay"

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self._token, self._exp = None, 0.0

    def _auth(self) -> str:
        if self._token and time.time() < self._exp - 60:
            return self._token
        cid, sec = os.environ.get("EBAY_CLIENT_ID"), os.environ.get("EBAY_CLIENT_SECRET")
        if not cid or not sec:
            raise RuntimeError("Mancano EBAY_CLIENT_ID / EBAY_CLIENT_SECRET")
        basic = base64.b64encode(f"{cid}:{sec}".encode()).decode()
        r = self.http.post(f"{API}/identity/v1/oauth2/token", timeout=20,
                           headers={"Authorization": f"Basic {basic}",
                                    "Content-Type": "application/x-www-form-urlencoded"},
                           data={"grant_type": "client_credentials",
                                 "scope": "https://api.ebay.com/oauth/api_scope"})
        r.raise_for_status()
        j = r.json()
        self._token, self._exp = j["access_token"], time.time() + int(j.get("expires_in", 7200))
        return self._token

    def search(self, query: str, max_price: float | None = None) -> list[Listing]:
        filt = ["buyingOptions:{FIXED_PRICE|BEST_OFFER}", "itemLocationCountry:IT", "priceCurrency:EUR"]
        if max_price:
            filt.append(f"price:[1..{max_price:.0f}]")
        r = self.http.get(f"{API}/buy/browse/v1/item_summary/search", timeout=25,
                          headers={"Authorization": f"Bearer {self._auth()}",
                                   "X-EBAY-C-MARKETPLACE-ID": "EBAY_IT"},
                          params={"q": query, "filter": ",".join(filt), "sort": "newlyListed", "limit": 50})
        r.raise_for_status()
        out = []
        for it in r.json().get("itemSummaries", []):
            try:
                price = float(it["price"]["value"])
            except (KeyError, ValueError):
                continue
            ship = None
            for so in it.get("shippingOptions", []):
                if "shippingCost" in so:
                    ship = float(so["shippingCost"]["value"]); break
            s = it.get("seller", {})
            out.append(Listing(
                site=self.name, id=it["itemId"], title=it.get("title", ""), price=price,
                url=it.get("itemWebUrl", ""), shipping=ship, buyer_fee=0.0,
                image=(it.get("image") or {}).get("imageUrl", ""),
                location=(it.get("itemLocation") or {}).get("city", ""),
                created=it.get("itemCreationDate", ""),
                seller_name=s.get("username", ""),
                seller_rating=(float(s["feedbackPercentage"]) / 100) if s.get("feedbackPercentage") else None,
                seller_reviews=s.get("feedbackScore"),
                extra={"condizione": it.get("condition", "")},
            ))
        return out
