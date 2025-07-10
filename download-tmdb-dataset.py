import asyncio
from collections import namedtuple
import aiohttp
from datetime import date
import os
import json
import random

async def discover_movies(session, start_date: date, end_date: date, page: int = 1):
    """
    Discover movies released between start_date and end_date.
    """
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
            async with session.get("/3/discover/movie", params=params) as response:
                movies = await response.json()
        except Exception as e:
            print(e)
            
            # If we encounter an error, we will retry with exponential backoff
            jitter = random.random()
            wait_secs = (2 ** tries) + jitter
            tries += 1
            await asyncio.sleep(wait_secs)
    
    return movies

def write_results(dirname, movies, start_date: date, end_date: date, page: int):
    """
    Write the results to a file.
    """
    os.makedirs(dirname, exist_ok=True)
    filename = f"{dirname}/{start_date.isoformat()}_{end_date.isoformat()}_page_{page:03}.json"
    with open(filename, "w") as f:
        f.write(json.dumps(movies))

DiscoverMoviesSlice = namedtuple("DiscoverMoviesSlice", ["start_date", "end_date", "total_pages", "movies"])

async def discover_movie_slices_between(session, start_date: date, end_date: date):

    while start_date < end_date:
        # Check the number of pages of movies released in the date range and reduce the date range until it's <= 500.
        slice_end_date = end_date
        while True:

            movies = await discover_movies(session, start_date, slice_end_date)
            
            if movies['total_pages'] <= 500:
                break

            # Else reduce the date range by half
            slice_end_date = start_date + (slice_end_date - start_date) // 2

        yield DiscoverMoviesSlice(start_date, slice_end_date, movies['total_pages'], movies)

        # Move the start date to the end date for the next iteration.
        start_date = slice_end_date

async def main():
    base_url = "https://api.themoviedb.org/"
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer " + os.getenv("TMDB_TOKEN", "")
    }
    dirname = "results"

    # Earliest movie release date in TMDb is 1874-12-09
    start_date = date(1874, 1, 1)
    async with aiohttp.ClientSession(base_url, headers=headers) as session:
        async for movie_slice in discover_movie_slices_between(session, start_date, date.today()):

            # We get the first page of movies "for free", so let's write it out before we start paginating for the rest.
            await asyncio.to_thread(write_results, dirname, movie_slice.movies, movie_slice.start_date, movie_slice.end_date, 1)

            # Now we can paginate through the rest of the pages.
            for page in range(2, movie_slice.total_pages + 1):
                if (page % 50 == 0):
                    print(f"> Fetching page {page} of {movie_slice.total_pages} for {movie_slice.start_date.isoformat()} to {movie_slice.end_date.isoformat()}...")
                movies = await discover_movies(session, movie_slice.start_date, movie_slice.end_date, page)
                if (page % 50 == 0):
                    print(f"- Writing page {page} of {movie_slice.total_pages} for {movie_slice.start_date.isoformat()} to {movie_slice.end_date.isoformat()}...")
                await asyncio.to_thread(write_results, dirname, movies, movie_slice.start_date, movie_slice.end_date, page)
                if (page % 50 == 0):
                    print(f"< Finished page {page} of {movie_slice.total_pages} for {movie_slice.start_date.isoformat()} to {movie_slice.end_date.isoformat()}...")
   
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())