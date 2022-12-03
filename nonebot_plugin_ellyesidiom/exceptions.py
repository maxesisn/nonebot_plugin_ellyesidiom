from nonebot import get_bots
import asyncio

class HashPrefixConflictError(Exception):
    def __init__(self, prefix: str, hash_list: list[str], gid: str) -> None:
        self.prefix = prefix
        self.hash_list = hash_list
        self.bot = get_bots().values().__iter__().__next__()
        self.gid = int(gid)
        self.loop = asyncio.get_event_loop()

    def __str__(self) -> str:
        message=f"HashPrefixConflictError: {self.prefix}"
        self.loop.run_until_complete(self.bot.send_group_msg(group_id=self.gid, message=message+"\n可选ID:\n"+'\n'.join(self.hash_list)))
        return message


class HashPrefixNotFoundError(Exception):
    def __init__(self, hash_prefix: str, gid: str) -> None:
        self.prefix = hash_prefix
        self.bot = get_bots().values().__iter__().__next__()
        self.gid = int(gid)
        self.loop = asyncio.get_event_loop()

    def __str__(self) -> str:
        message=f"HashPrefixNotFoundError: {self.prefix}"
        self.loop.run_until_complete(self.bot.send_group_msg(group_id=self.gid, message=message))
        return message