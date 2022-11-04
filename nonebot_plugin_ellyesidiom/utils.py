import base64
import math
from nonebot import get_driver
import pytz

global_config = get_driver().config
shanghai_tz = pytz.timezone('Asia/Shanghai')

# base16 to base32
async def base16_to_base32(base16: str) -> str:
    return base64.b32encode(bytearray.fromhex(base16)).decode('utf-8').replace('=', '')

async def base32_to_base16(base32: str) -> str:
    pad_length = math.ceil(len(base32) / 8) * 8 - len(base32)
    base32 = base32 + '=' * pad_length
    return base64.b32decode(base32).hex()

