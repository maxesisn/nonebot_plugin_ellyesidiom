from nonebot import get_driver
import pytz

global_config = get_driver().config
shanghai_tz = pytz.timezone('Asia/Shanghai')

tips_no_permission = "您没有权限使用此命令。"