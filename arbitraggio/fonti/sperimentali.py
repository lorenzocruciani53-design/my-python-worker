"""Vinted, Subito, Wallapop: moduli SPERIMENTALI.

Questi siti non hanno un'API pubblica: i moduli usano gli stessi endpoint
interni che usa il loro sito web. Non è stato possibile collaudarli dal
server di sviluppo, quindi sono DISATTIVATI di default in config.json.
Attivali uno alla volta sul tuo PC/VPS e controlla con:
    python -m arbitraggio --prova vinted "ps5 slim"
Se un sito cambia formato, il modulo stampa l'errore e il bot continua con gli altri.
Nota: l'uso automatizzato può essere contrario ai termini dei siti; tieni
intervalli lunghi (config: "pausa_tra_ricerche_sec") e usali a tuo rischio.
"""
from __future__ import annotations

from .base import Listing, Source


def _f(v) -> float | None:
    try:
        if isinstance(v, dict):
            v = v.get("amount")
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


class Vinted(Source):
    name = "vinted"
    BASE = "https://www.vinted.it"

    def _session(self):
        if not self.http.cookies.get("access_token_web"):
            self.http.get(self.BASE, timeout=20)

    def search(self, query, max_price=None):
        self._session()
        params = {"search_text": query, "order": "newest_first", "per_page": 96, "currency": "EUR"}
        if max_price:
            params["price_to"] = int(max_price)
        r = self.http.get(f"{self.BASE}/api/v2/catalog/items", params=params, timeout=25,
                          headers={"Accept": "application/json"})
        if r.status_code == 401:
            self.http.cookies.clear(); self._session()
            r = self.http.get(f"{self.BASE}/api/v2/catalog/items", params=params, timeout=25)
        r.raise_for_status()
        out = []
        for it in r.json().get("items", []):
            price = _f(it.get("price"))
            if price is None:
                continue
            total = _f(it.get("total_item_price"))
            fee = _f(it.get("service_fee"))
            if fee is None and total is not None:
                fee = round(total - price, 2)
            u = it.get("user") or {}
            out.append(Listing(
                site=self.name, id=str(it["id"]), title=it.get("title", ""), price=price,
                url=it.get("url") or f"{self.BASE}/items/{it['id']}", buyer_fee=fee,
                image=((it.get("photo") or {}).get("url") or ""),
                seller_name=u.get("login", ""), seller_id=str(u.get("id", "")),
                extra={"condizione": it.get("status", "")},
            ))
        return out

    def enrich_seller(self, item):
        if not item.seller_id:
            return
        r = self.http.get(f"{self.BASE}/api/v2/users/{item.seller_id}", timeout=20)
        if r.ok:
            u = r.json().get("user", {})
            item.seller_rating = u.get("feedback_reputation")
            item.seller_reviews = u.get("feedback_count")
            item.extra["venditore_annunci"] = u.get("item_count")
            item.extra["venditore_citta"] = u.get("city")


class Subito(Source):
    name = "subito"
    API = "https://hades.subito.it/v1/search/items"

    def search(self, query, max_price=None):
        params = {"q": query, "c": self.cfg.get("categoria", 44), "t": "s", "lim": 50, "sort": "datedesc",
                  "shp": "true" if self.cfg.get("solo_spedibili", True) else "false"}
        if max_price:
            params["pe"] = int(max_price)
        r = self.http.get(self.API, params=params, timeout=25, headers={"X-Subito-Channel": "web"})
        r.raise_for_status()
        out = []
        for ad in r.json().get("ads", []):
            feats = {f.get("uri"): f for f in ad.get("features", [])}
            pv = (feats.get("/price") or {}).get("values") or [{}]
            price = _f(pv[0].get("key"))
            if price is None:
                continue
            imgs = ad.get("images") or [{}]
            geo = ad.get("geo") or {}
            adv = ad.get("advertiser") or {}
            out.append(Listing(
                site=self.name, id=str(ad.get("urn", "")).split(":")[-1], title=ad.get("subject", ""),
                price=price, url=(ad.get("urls") or {}).get("default", ""),
                description=ad.get("body", "")[:1500],
                image=(imgs[0].get("cdn_base_url") or imgs[0].get("scale", [{}])[-1].get("uri", "") if imgs else ""),
                location=(geo.get("town") or {}).get("value", ""), created=(ad.get("dates") or {}).get("display", ""),
                seller_name=adv.get("name", ""), seller_id=str(adv.get("user_id", "")),
                extra={"professionista": bool(adv.get("company"))},
            ))
        return out


class Wallapop(Source):
    name = "wallapop"
    API = "https://api.wallapop.com/api/v3"

    def search(self, query, max_price=None):
        params = {"keywords": query, "order_by": "newest", "source": "search_box",
                  "latitude": self.cfg.get("lat", 41.9), "longitude": self.cfg.get("lon", 12.5)}
        if max_price:
            params["max_sale_price"] = int(max_price)
        r = self.http.get(f"{self.API}/search", params=params, timeout=25,
                          headers={"X-DeviceOS": "0", "Accept": "application/json"})
        r.raise_for_status()
        j = r.json()
        items = (((j.get("data") or {}).get("section") or {}).get("payload") or {}).get("items") \
            or j.get("search_objects", [])
        out = []
        for it in items:
            price = _f(it.get("price"))
            if price is None:
                continue
            imgs = it.get("images") or [{}]
            out.append(Listing(
                site=self.name, id=str(it.get("id")), title=it.get("title", ""), price=price,
                url=f"https://it.wallapop.com/item/{it.get('web_slug', it.get('id'))}",
                description=(it.get("description") or "")[:1500],
                image=((imgs[0].get("urls") or {}).get("medium", "")),
                seller_id=str(it.get("user_id", "")),
            ))
        return out

    def enrich_seller(self, item):
        if not item.seller_id:
            return
        r = self.http.get(f"{self.API}/users/{item.seller_id}/stats", timeout=20)
        if r.ok:
            j = r.json()
            ratings = {x.get("type"): x.get("value") for x in j.get("ratings", [])}
            counters = {x.get("type"): x.get("value") for x in j.get("counters", [])}
            if ratings.get("reviews") is not None:
                item.seller_rating = float(ratings["reviews"]) / 100
            item.seller_reviews = counters.get("reviews")
