import asyncio
from datetime import date, timedelta
from themoviedb import aioTMDb, utils
import os
import json

async def discover_movies(tmdb, start_date: date, end_date: date, page: int = 1):
    """
    Discover movies released between start_date and end_date.
    """
    if (page % 50 == 0):
        print(f"> Starting {start_date.isoformat()} to {end_date.isoformat()}: Page {page}")
    
    movies = None
    wait_secs = 0
    while movies is None:
        try:
            movies = await tmdb.discover().movie(
                sort_by="primary_release_date.asc",
                primary_release_date__gte=start_date.isoformat(),
                primary_release_date__lte=end_date.isoformat(),
                page=page
            )
        except Exception as e:
            print(e)
            wait_secs += 1
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
    filename = f"tmdb_dump/{start_date.isoformat()}_{end_date.isoformat()}_page_{page}.json"
    with open(filename, "w") as f:
        f.write(json.dumps(utils.as_dict(movies)))

async def main():
    tmdb = aioTMDb()

    # Earliest movie release date in TMDb is 1874-12-09
    start_date = date(1874, 1, 1)
    results = []
    coros = []
    
    while start_date < date.today():
        # Check the number of movies released in the date range
        # and reduce the date range until it's less than 500
        end_date = date.today()
        while True:

            movies = await tmdb.discover().movie(
                sort_by="primary_release_date.asc",
                primary_release_date__gte=start_date.isoformat(),
                primary_release_date__lte=end_date.isoformat())
            
            if movies.total_pages and movies.total_pages < 500:
                break

            # Reduce the date range by half
            delta = (end_date - start_date) // 2
            end_date = end_date - delta

        results.append(movies)
        coros.extend([
            discover_movies(tmdb, start_date, end_date, page)
            for page in range(2, (movies.total_pages or 1) + 1)
        ])
        start_date = end_date

    results.extend(await asyncio.gather(*coros))
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())