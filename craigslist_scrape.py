import asyncio, aiohttp, ssl, re, random, time, json
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlencode, urljoin

URL_BASE = "https://pittsburgh.craigslist.org" #url with zip code/lat-lon parameter goes to that region's craiglist page automatically
VERIFY_PATH = "C:/Projects/Scraper/company-ca.pem"

postal = 94539            
search_term = "chair"
search_radius = 15
max_price = -1
min_price = -1
title_only = True    
free = False
search_url = ""

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

prices, titles, links, lats, lons, rgns, cities, img_ars = [],[],[],[],[],[],[],[]
CONC_LIMIT = 10

#constructs url based on chosen parameters
def get_url(max_price):
    qs = {"bundleDuplicates": 1,
        "hasPic": 1,
        "query": search_term,
        "search_distance": search_radius,
        "postal": postal,
        "sort": "pricedsc"}
    if title_only:
        qs["srchType"] = "T"
    if free:
        qs["free"] = "T"
    if max_price >= 0:
        qs["max_price"] = max_price
    if min_price >= 0:
        qs["min_price"] = min_price
    return f"{URL_BASE}/search/sss?{urlencode(qs)}#search=2~gallery~0"

def get_more_info(soup):
    json_tag = soup.select_one("#ld_searchpage_results")
    if not json_tag:
        return []
    
    #get raw json
    raw_json = json_tag.string or json_tag.get_text()
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as e:
        print("json decode error: ",e)
        return []
    items = data.get("itemListElement", [])

    for el in items:
        item = el.get("item")
        #name = item.get('name')
        #cur = item.get('offers').get('priceCurrency')
        #price = item.get('offers').get('price')
        loc = item.get('offers').get('availableAtOrFrom')
        lat = loc.get('geo').get('latitude')
        lon = loc.get('geo').get('longitude')
        rgn = loc.get('address').get('addressRegion')
        city = loc.get('address').get('addressLocality')
        img_els = item.get("image")
        imgs = []
        for iel in img_els:
            imgs.append(iel)   

        lats.append(lat)
        lons.append(lon)
        rgns.append(rgn)
        cities.append(city)
        img_ars.append(imgs)
        print(lat, lon, rgn, city)

    return 0

async def polite_get(session, url, ssl_ctx, referer = None, tries = 4):
    backoff = 1.0
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
        await asyncio.sleep(random.uniform(0.4, 0.8))
        if not html:
            return {"url": url, "blocked": True}
    soup_listing = BeautifulSoup(html, "html.parser")
    
    location_el = soup_listing.find('div', id='map')
    lat = location_el.get('data-latitude')
    lon = location_el.get('data-longitude')
    lats.append(lat)
    lons.append(lon)

    city_el = soup_listing.find('meta', attrs={"name": "geo.placename"})
    city = city_el.get('content')
    cities.append(city)

    rgn_el = soup_listing.find('meta', attrs={"name": "geo.region"})
    rgn = rgn_el.get('content').split('-')[1]
    rgns.append(rgn)

    imgs = []
    img_elements = soup_listing.find_all("div", class_="slide")
    for img_el in img_elements:
        img = img_el.find("img")
        if(img):
            imgs.append(img.get('src'))
    img_ars.append(imgs)
    print(lat, lon, rgn)

    return 0

async def scrape_list_conc(links, referer):
    ssl_ctx = ssl.create_default_context(cafile=VERIFY_PATH)  # trust corp CA
    sem = asyncio.Semaphore(CONC_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [parse_listing(session, l, sem, ssl_ctx, referer) for l in links]
        return await asyncio.gather(*tasks)

def user_search():
    return 0

def main():
    user_search()

    repeat = True
    last_price_limit = -1
    while repeat:
        search_url = get_url(last_price_limit)
        print("TO SEARCH: ", search_url)

        r = requests.get(search_url, headers=HEADERS, verify=VERIFY_PATH, timeout=25)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        listings = soup.find_all('li', class_='cl-static-search-result')

        free_links = []

        listing_cnt = 0
        for listing in listings:
            listing_cnt+=1
            price_element = listing.find('div', class_='price')
            price = int((price_element.text).replace("$", "").replace(",", "").strip())
            title = listing.get("title")
            # if titles and prices and title == titles[-1] and price == prices[-1]:
            #     continue
            prices.append(price)
            titles.append(title)
            link = listing.find("a")["href"]   
            links.append(link) 
            if(price == 0):
                free_links.append(link)
            print(title, price, link)

        get_more_info(soup)

        asyncio.run(scrape_list_conc(free_links, referer=search_url))

        last_price_limit = (int)(prices[-1])
        if last_price_limit == 0 or listing_cnt == 0:
            repeat = False

    listing_df = pd.DataFrame(
        {
            'title': titles,
            'price': prices,
            'link': links,
            'latitude': lats,
            'longitude': lons,
            'region': rgns,
            'city': cities,
            'images': img_ars
        }
    )

    listing_df = listing_df.drop_duplicates(subset=["title", "price", "latitude", "longitude"], keep="first")

    print(listing_df)
    listing_df.to_csv("listing_data.csv")

if __name__ == "__main__":
    main()