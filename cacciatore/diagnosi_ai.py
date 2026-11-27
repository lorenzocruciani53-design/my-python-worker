"""Diagnosi: prova gli indirizzi conosciuti di GitHub Models e stampa cosa risponde ciascuno."""
import json
import os

import requests

tok = os.environ.get("GITHUB_TOKEN", "")
print("GITHUB_TOKEN presente:", bool(tok), "lunghezza", len(tok))
H = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json",
     "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
msg = [{"role": "user", "content": 'Rispondi solo con il JSON {"ok": true}'}]


def prova(nome, metodo, url, body=None, headers=None):
    try:
        r = requests.request(metodo, url, json=body, headers=headers or H, timeout=60, allow_redirects=False)
        print(f"\n== {nome}\n{metodo} {url}\nHTTP {r.status_code} · {r.headers.get('content-type')} · "
              f"location={r.headers.get('location')}\n{r.text[:600]!r}")
    except Exception as ex:
        print(f"\n== {nome}: ERRORE {type(ex).__name__}: {ex}")


prova("catalogo modelli", "GET", "https://models.github.ai/catalog/models")
for modello in ["openai/gpt-4.1-mini", "openai/gpt-4o-mini"]:
    prova(f"nuovo endpoint {modello}", "POST", "https://models.github.ai/inference/chat/completions",
          {"model": modello, "messages": msg})
prova("endpoint con owner", "POST",
      f"https://models.github.ai/orgs/{os.environ.get('GITHUB_REPOSITORY_OWNER', '')}/inference/chat/completions",
      {"model": "openai/gpt-4.1-mini", "messages": msg})
prova("vecchio endpoint Azure", "POST", "https://models.inference.ai.azure.com/chat/completions",
      {"model": "gpt-4o-mini", "messages": msg},
      {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
print("\nproxy:", {k: v for k, v in os.environ.items() if "proxy" in k.lower()})
