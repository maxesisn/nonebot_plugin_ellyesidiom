import fuzzywuzzy.process as fuzz
from .data_mongo import get_all_alias

async def ep_alias_to_id(name: str) -> str | None:
    ep_alias: dict[str, list[str]] = await get_all_alias()
    for ep, alias in ep_alias.items():
        if name in alias:
            return ep
        if fuzz.extractOne(name, alias)[1] > 80:
            return ep
    return None

async def id_to_ep_alias(id: str) -> str | None:
    ep_alias: dict[str, list[str]] = await get_all_alias()
    for ep, alias in ep_alias.items():
        if id == ep:
            return alias[0]
    return None