"""
forceid.py — Test d'énumération d'IDs sur ta propre API (mode aléatoire)
Usage : python forceid.py --url "https://ton-api.com/user/{code}" --workers 50
"""

import asyncio
import aiohttp
import argparse
import itertools
import random
import time
from pathlib import Path


# ─── Configuration ────────────────────────────────────────────────────────────

DEFAULT_URL       = "https://ton-api.com/user/{code}"
NOT_FOUND_TEXTS   = ["user not found", "database error"]
OUTPUT_FILE       = "found_codes.txt"
DELAY_BETWEEN_REQ = 0.0
TIMEOUT_SECONDS   = 10
CHUNK_SIZE        = 500


# ─── Générateur de codes aléatoires ───────────────────────────────────────────

def generate_codes(start: str = "0000-0000-0000", end: str = "9999-9999-9999"):
    """Génère des codes aléatoires (avec possible doublon) entre start et end."""
    def code_to_int(code: str) -> int:
        return int(code.replace("-", ""))

    def int_to_code(n: int) -> str:
        s = f"{n:012d}"
        return f"{s[0:4]}-{s[4:8]}-{s[8:12]}"

    start_n = code_to_int(start)
    end_n   = code_to_int(end)

    seen = set()
    total = end_n - start_n + 1

    while len(seen) < total:
        n = random.randint(start_n, end_n)
        if n not in seen:
            seen.add(n)
            yield int_to_code(n)


# ─── Worker async ──────────────────────────────────────────────────────────────

async def check_code(session: aiohttp.ClientSession, url_template: str, code: str,
                     ignore_list: list, results: list, semaphore: asyncio.Semaphore):
    url = url_template.replace("{code}", code)
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as resp:
                body = await resp.text()
                status = resp.status
                body_lower = body.lower()

                if not any(text in body_lower for text in ignore_list):
                    result = f"[HIT] Code={code} | Status={status} | Response={body[:200]}"
                    print(f"\033[92m{result}\033[0m")
                    results.append(f"{code} | {status} | {body[:200]}\n")
                else:
                    print(f"\033[90m[miss] {code}\033[0m", end="\r")

        except asyncio.TimeoutError:
            print(f"\033[93m[timeout] {code}\033[0m", end="\r")
        except Exception as e:
            print(f"\033[91m[error] {code}: {e}\033[0m", end="\r")

        if DELAY_BETWEEN_REQ > 0:
            await asyncio.sleep(DELAY_BETWEEN_REQ)


# ─── Main ──────────────────────────────────────────────────────────────────────

async def run(url_template: str, start: str, end: str, workers: int, ignore_list: list):
    results = []
    semaphore = asyncio.Semaphore(workers)
    total = int(end.replace("-", "")) - int(start.replace("-", "")) + 1

    print(f"\n{'─'*60}")
    print(f"  URL      : {url_template}")
    print(f"  Plage    : {start} → {end}")
    print(f"  Total    : {total:,} codes")
    print(f"  Workers  : {workers}")
    print(f"  Ignorés  : {ignore_list}")
    print(f"  Mode     : aléatoire")
    print(f"  Chunk    : {CHUNK_SIZE} codes/lot")
    print(f"{'─'*60}\n")

    start_time = time.time()
    connector = aiohttp.TCPConnector(limit=workers)

    async with aiohttp.ClientSession(connector=connector) as session:
        code_gen = generate_codes(start, end)
        done = 0

        while True:
            chunk = list(itertools.islice(code_gen, CHUNK_SIZE))
            if not chunk:
                break

            tasks = [
                check_code(session, url_template, code, ignore_list, results, semaphore)
                for code in chunk
            ]
            await asyncio.gather(*tasks)

            done += len(chunk)
            elapsed = time.time() - start_time
            speed = done / elapsed if elapsed > 0 else 0
            pct = done / total * 100
            print(f"  Progression : {done:,} / {total:,} ({pct:.4f}%) | {speed:.0f} req/s", end="\r")

            if results and done % 10_000 < CHUNK_SIZE:
                Path(OUTPUT_FILE).write_text("".join(results), encoding="utf-8")
                print(f"\n  💾 Sauvegarde intermédiaire : {len(results)} résultat(s)")

    elapsed = time.time() - start_time

    if results:
        Path(OUTPUT_FILE).write_text("".join(results), encoding="utf-8")
        print(f"\n\n✅ {len(results)} code(s) trouvé(s) → sauvegardé dans '{OUTPUT_FILE}'")
    else:
        print("\n\nAucun code valide trouvé.")

    print(f"⏱  Durée : {elapsed:.1f}s | Vitesse moyenne : {total/elapsed:.0f} req/s\n")


# ─── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test d'énumération d'IDs sur ta propre API")
    parser.add_argument("--url",       default=DEFAULT_URL,          help="URL avec {code} comme placeholder")
    parser.add_argument("--start",     default="0000-0000-0000",     help="Code de départ")
    parser.add_argument("--end",       default="9999-9999-9999",     help="Code de fin")
    parser.add_argument("--workers",   type=int, default=50,         help="Nombre de requêtes parallèles")
    parser.add_argument("--not-found", default=None,                 help="Textes à ignorer séparés par virgule")
    parser.add_argument("--output",    default=OUTPUT_FILE,          help="Fichier de sortie")
    parser.add_argument("--chunk",     type=int, default=CHUNK_SIZE, help="Taille des lots en mémoire")
    args = parser.parse_args()

    OUTPUT_FILE = args.output
    CHUNK_SIZE  = args.chunk

    if args.not_found:
        ignore_list = [t.strip().lower() for t in args.not_found.split(",")]
    else:
        ignore_list = [t.lower() for t in NOT_FOUND_TEXTS]

    asyncio.run(run(
        url_template = args.url,
        start        = args.start,
        end          = args.end,
        workers      = args.workers,
        ignore_list  = ignore_list,
    ))
