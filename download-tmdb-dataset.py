import asyncio
import aiohttp
from datetime import date
import os
import json
import random

async def discover_movies(session, start_date: date, end_date: date, page: int = 1):
    """
    Discover movies released between start_date and end_date.
    """
    if (page % 50 == 0):
        print(f"> Starting {start_date.isoformat()} to {end_date.isoformat()}: Page {page}")
    
    movies = None
    tries = 1
    while movies is None:
        try:
            params = {
                "sort_by": "primary_release_date.asc",
                "primary_release_date.gte": start_date.isoformat(),
                "primary_release_date.lte": end_date.isoformat(),
                "page": page
            }
            async with session.get(params=params) as response:
                movies = await response.json()
        except Exception as e:
            print(e)
            
            # If we encounter an error, we will retry with exponential backoff
            jitter = random.random()
            wait_secs = (2 ** tries) + jitter
            tries += 1
            await asyncio.sleep(wait_secs)

    if (page % 50 == 0):
        print(f"- Writing {start_date.isoformat()} to {end_date.isoformat()}: Page {page}")

    # Write the results to a file
    await asyncio.to_thread(write_results, movies, start_date, end_date, page)

    if (page % 50 == 0):
        print(f"< Finished {start_date.isoformat()} to {end_date.isoformat()}: Page {page}")

    return movies

def write_results(movies, start_date: date, end_date: date, page: int):
    """
    Write the results to a file.
    """
    os.makedirs("tmdb_dump", exist_ok=True)
    filename = f"tmdb_dump/{start_date.isoformat()}_{end_date.isoformat()}_page_{page:03}.json"
    with open(filename, "w") as f:
        f.write(json.dumps(movies))

async def discover_movies_from(session, start_date: date):
    results = []
    coros = []

    while start_date < date.today():
        # Check the number of movies released in the date range
        # and reduce the date range until it's less than 500
        end_date = date.today()
        while True:

            params = {
                "sort_by": "primary_release_date.asc",
                "primary_release_date.gte": start_date.isoformat(),
                "primary_release_date.lte": end_date.isoformat()
            }
            async with session.get(params=params) as response:
                movies = await response.json()
            
            if 'total_pages' in movies and movies['total_pages'] < 500:
                print(f"Found {movies['total_results']} movies from {start_date.isoformat()} to {end_date.isoformat()}")
                write_results(movies, start_date, end_date, 1)
                break

            # Reduce the date range by half
            delta = (end_date - start_date) // 2
            end_date = end_date - delta

        results.append(movies)
        coros.extend([
            discover_movies(session, start_date, end_date, page)
            for page in range(2, (movies['total_pages']) + 1)
        ])
        start_date = end_date

    results.extend(await asyncio.gather(*coros))

async def main():
    url = "https://api.themoviedb.org/3/discover/movie"
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer " + os.getenv("TMDB_TOKEN", "")
    }

    # Earliest movie release date in TMDb is 1874-12-09
    start_date = date(1874, 1, 1)
    async with aiohttp.ClientSession(url, headers=headers) as session:
        await discover_movies_from(session, start_date)
   
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())