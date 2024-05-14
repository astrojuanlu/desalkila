import httpx
import typing as t
from anyio import sleep, create_task_group, Semaphore

from structlog import get_logger

BASE_URL_TPL = (
    "https://us1.locationiq.com/v1/search?key={api_key}&q={query}&format=json"
)

logger = get_logger()


async def fetch_location(
    query: str, api_key: str, *, client=None, base_url_tpl=BASE_URL_TPL
):
    if client is None:
        client = httpx.AsyncClient()

    url = base_url_tpl.format(query=query, api_key=api_key)
    request = client.build_request("GET", url)

    response = await client.send(request)
    response.raise_for_status()

    return response.json()


async def _fetch_to_results(location, api_key, *, client, results, semaphore):
    async with semaphore:
        if location in results:
            logger.info("Skipping, location cached", location=location)
        else:
            logger.info("Fetching", location=location)
            try:
                places = await fetch_location(location, api_key, client=client)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "Error while fetching location", exc=exc, url=exc.request.url
                )
            else:
                logger.debug(
                    "Got places for location", location=location, places=places
                )
                results[location] = places

            # FIXME: Not very robust, requires several passes
            # We either make it more conservative or we implement proper rate limiting
            # See https://github.com/deknowny/rate-limit-semaphore/blob/main/ralisem/impls.py
            # and https://github.com/agronholm/anyio/blob/master/src/anyio/_core/_synchronization.py
            # for inspiration
            # The logic could be
            # If in the last 1 second there has been less than 2 queries, proceed
            # otherwise, wait until last_query_timestamp + 1 second
            logger.debug("Waiting")
            await sleep(0.8)


async def fetch_all_locations(
    locations: list,
    api_key: str,
    results: dict[str, list[dict[str, t.Any]]] | None = None,
    client=None,
):
    if not results:
        results = dict()

    if client is None:
        client = httpx.AsyncClient()

    semaphore = Semaphore(2)

    try:
        async with create_task_group() as tg:
            for location in locations:
                tg.start_soon(
                    lambda location,
                    api_client,
                    client,
                    results,
                    semaphore: _fetch_to_results(
                        location,
                        api_client,
                        client=client,
                        results=results,
                        semaphore=semaphore,
                    ),
                    location,
                    api_key,
                    client,
                    results,
                    semaphore,
                )
    except* Exception as excgroup:
        # AFAIU The moment one error happens in any task, it will cancel all the others
        for exc in excgroup.exceptions:
            logger.exception("Unexpected error while fetching location")

    return results
