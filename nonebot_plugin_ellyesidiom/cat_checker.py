# ptcp stands for Participants
import fuzzywuzzy.process as fuzz

core_ep = {
    "ellye": "491673070",
    "corvo": "269077688",
    "ark": "1006205255",
    "poppy": "574866115",
    "latte": "562978277",
    "coffee": "438971718",
    "rikka": "779634201",
    "meals": "meals"
}

ep_alias = {
    "ellye": ["怡宝", "e宝", "e"],
    "corvo": ["查理", "查理酱"],
    "ark": ["方舟", "方院士", "方教授"],
    "poppy": ["Poppy", "Poppy酱"],
    "latte": ["拿铁", "拿铁麻麻"],
    "coffee": ["咖啡"],
    "rikka": ["六花"],
    "meals": ["怡宴"]
}

async def ep_alias_to_id(name: str) -> str:
    for ep, alias in ep_alias.items():
        if name in alias:
            return core_ep[ep]
        if fuzz.extractOne(name, alias)[1] > 80:
            return core_ep[ep]
    return None

async def id_to_ep_alias(id: str) -> str:
    for ep, ep_id in core_ep.items():
        if id == ep_id:
            return ep_alias[ep][0]
    return None