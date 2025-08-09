#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scrape Etsy shop listings, read reviewCount from JSON-LD on each listing page,
# sort by review count and export TOP 100 as CSV. Uses Playwright (headless Chromium).

import asyncio
import csv
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

SHOP_DEFAULT = "https://www.etsy.com/shop/PatternsAndStitches?ref=shop-header-name&listing_id=1725310400&from_page=listing"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

LISTING_REGEX = re.compile(r"/listing/(\d+)")

@dataclass
class ListingInfo:
    url: str
    title: str
    reviews: int


def normalize_listing_url(href: str) -> Optional[str]:
    if not href:
        return None
    m = LISTING_REGEX.search(href)
    if not m:
        return None
    base = href.split("?")[0]
    return base


async def collect_listing_urls(page, max_pages: Optional[int] = None, hard_limit: int = 5000) -> List[str]:
    """From a shop page, paginate through items and collect unique /listing/<id> URLs."""
    urls = []
    seen = set()
    page_idx = 1

    while True:
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(random.uniform(0.8, 1.6))

        anchors = await page.eval_on_selector_all('a[href*="/listing/"]', 'els => els.map(e => e.href)')
        for href in anchors:
            u = normalize_listing_url(href)
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
                if len(urls) >= hard_limit:
                    break
        if len(urls) >= hard_limit:
            break

        # Try to go to next page
        next_sel_list = [
            'a[aria-label="Next page"]',
            'a[aria-label="Next"]',
            'a[rel="next"]',
            'nav a[href*="page="]',
        ]
        next_link = None
        for sel in next_sel_list:
            try:
                el = await page.query_selector(sel)
                if el:
                    next_link = el
                    break
            except Exception:
                pass

        if not next_link:
            # last attempt by text contains
            try:
                next_link = await page.query_selector("xpath=//a[contains(., 'Next')]")
            except Exception:
                next_link = None

        if not next_link:
            break

        page_idx += 1
        if max_pages and page_idx > max_pages:
            break

        await next_link.scroll_into_view_if_needed()
        await asyncio.sleep(random.uniform(0.2, 0.6))
        try:
            await next_link.click()
            await page.wait_for_load_state("networkidle")
        except PWTimeoutError:
            break

    return urls


def extract_from_jsonld_blocks(blocks: List[str]) -> Tuple[str, int]:
    title = None
    review_count = 0
    for txt in blocks:
        t = txt.strip()
        if "aggregateRating" not in t and '"@type":"Product"' not in t and '"@type": "Product"' not in t:
            continue
        # extract title
        m_title = re.search(r'"name"\s*:\s*"([^"]+)"', t)
        if m_title and not title:
            title = m_title.group(1)
        # extract reviewCount
        m_count = re.search(r'"reviewCount"\s*:\s*"?(\d+)"?', t)
        if m_count:
            try:
                review_count = int(m_count.group(1))
            except ValueError:
                pass
            break
    if not title:
        title = "(untitled)"
    return title, review_count


async def fetch_listing_info(context, url: str, timeout_ms: int = 45000) -> ListingInfo:
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        # let dynamic content settle a bit
        await asyncio.sleep(random.uniform(0.6, 1.2))
        # grab JSON-LD
        blocks = await page.eval_on_selector_all(
            'script[type="application/ld+json"]',
            'els => els.map(e => e.textContent || "")'
        )
        title, reviews = extract_from_jsonld_blocks(blocks)
        return ListingInfo(url=url, title=title, reviews=reviews)
    except Exception:
        return ListingInfo(url=url, title="(error)", reviews=0)
    finally:
        await page.close()


async def run(shop_url: str, out_csv: str, max_pages: Optional[int], concurrency: int):
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="en-US",
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 2000},
        )
        page = await context.new_page()
        await page.goto(shop_url, wait_until="networkidle", timeout=60000)

        listing_urls = await collect_listing_urls(page, max_pages=max_pages)
        # de-dup already done; shuffle lightly to distribute load
        random.shuffle(listing_urls)

        sem = asyncio.Semaphore(concurrency)
        results: List[ListingInfo] = []

        async def worker(u: str):
            async with sem:
                info = await fetch_listing_info(context, u)
                results.append(info)
                await asyncio.sleep(random.uniform(0.4, 0.9))

        await asyncio.gather(*(worker(u) for u in listing_urls))

        # sort by reviews desc and top 100
        results.sort(key=lambda x: x.reviews, reverse=True)
        top100 = results[:100]

        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["rank", "reviews", "title", "url"])
            for idx, r in enumerate(top100, 1):
                w.writerow([idx, r.reviews, r.title, r.url])

        # quick human-readable preview in CI logs
        print("\nTOP 20:")
        for idx, r in enumerate(top100[:20], 1):
            print(f"{idx:>2}. [{r.reviews} reviews] {r.title} -> {r.url}")

        await context.close()
        await browser.close()


def parse_args(argv: List[str]):
    import argparse
    p = argparse.ArgumentParser(description="Export top 100 Etsy listings by review count.")
    p.add_argument("--shop", default=SHOP_DEFAULT, help="Etsy shop URL (landing page)")
    p.add_argument("--out", default="data/patternsandstitches_top100_reviews.csv", help="Output CSV path")
    p.add_argument("--max-pages", type=int, default=None, help="Limit number of shop pages to crawl")
    p.add_argument("--concurrency", type=int, default=4, help="Concurrent listing fetches")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    asyncio.run(run(args.shop, args.out, args.max_pages, args.concurrency))
