from playwright.async_api import async_playwright
import asyncio, aiohttp, ssl, re, random
import requests, certifi
from requests import get
from bs4 import BeautifulSoup
import re
import pandas as pd
import math
from datetime import datetime, date, time, timedelta

url_base = "http://pittsburgh.craigslist.org/search/sss?bundleDuplicates=1&" #url with zip code/lat-lon parameter goes to that region's craiglist page automatically
postal = 94539
lat = 40.468
lon = -79.9
search_term = "couch"
search_radius = 15
title_only = True
if(title_only):
    url_base = url_base + "srchType=T&"

VERIFY_PATH = "C:/Projects/Scraper/company-ca.pem"
HEADERS = {"User-Agent": "Mozilla/5.0",
           "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Language": "en-US,en;q=0.9"}
full_url = url_base + "postal=" + str(postal) + "&query=" + search_term + "&search_distance=" + str(search_radius)

CONC_LIMIT = 8

titles = []
links = []
prices = []

async def polite_get(session, url, ssl_ctx, referer = None, tries = 4):
    backoff = 1.2
    for i in range(tries):
        try:
            headers = dict(HEADERS)
            if referer:
                headers["Referer"] = referer
            async with session.get(url, headers=headers, ssl=ssl_ctx, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status in (403, 429, 503):
                    # cooldown on block / rate limit
                    await asyncio.sleep(backoff + random.uniform(0, 0.8))
                    backoff *= 2
                    continue
                resp.raise_for_status()
                return await resp.text()
        except Exception:
            await asyncio.sleep(backoff + random.uniform(0, 0.8))
            backoff *= 2
    return None

async def parse_listing(session, url, sem, ssl_ctx, referer):
    async with sem:
        html = await polite_get(session, url, ssl_ctx, referer=referer)
        #async with session.get(url, headers=HEADERS, ssl=ssl_ctx, timeout=aiohttp.ClientTimeout(total=25)) as resp:
        await asyncio.sleep(random.uniform(0.4, 1.2))
        if not html:
            return {"url": url, "blocked": True}
    soup_listing = BeautifulSoup(html, "html.parser")
    date_posted = None
    posted_dates = soup_listing.find_all('time', class_='date timeago')
    if(len(posted_dates) >= 2):
        date_posted = posted_dates[1].get("datetime")
    else:
        date_posted = posted_dates[0].get("datetime")
    print(date_posted)

    location_element = soup_listing.find('div', id='map')
    lat = location_element.get('data-latitude')
    lon = location_element.get('data-longitude')
    print(lat)
    print(lon)

    img_elements = soup_listing.find_all("div", class_="slide")
    for img_element in img_elements:
        img = img_element.find("img")
        if(img):
            print(img.get("src"))

    return {"date_posted": date_posted, "lat": lat, "lon": lon}

async def scrape_list_conc(links):
    ssl_ctx = ssl.create_default_context(cafile=VERIFY_PATH)  # trust your corp CA
    sem = asyncio.Semaphore(CONC_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [parse_listing(session, l, sem, ssl_ctx, l) for l in links]
        return await asyncio.gather(*tasks)

def main():
    print(full_url)
    resp_search = requests.get(full_url, headers=HEADERS, verify=VERIFY_PATH)
    soup_outer = BeautifulSoup(resp_search.text, "html.parser")

    listings = soup_outer.find_all('li', class_='cl-static-search-result')
    for listing in listings:
        price_element = listing.find('div', class_='price')
        if price_element:
            price = int((price_element.text).replace("$", "").replace(",", "").strip())
        else:
            continue
        title = listing.get("title")
        link = listing.find("a")["href"]    
        
        titles.append(title)
        links.append(link)
        prices.append(price)

        print(title)
        print(link)
        print(price)

    details = asyncio.run(scrape_list_conc(links))

if __name__ == '__main__':
    main()


