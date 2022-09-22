import asyncio
from nonebot import on_command, get_driver
from nonebot.adapters.onebot.v11 import Bot, Event, Message, MessageSegment
from nonebot.params import CommandArg
from nonebot.rule import to_me
import re

import httpx

global_config = get_driver().config

tg_bot_token:str = global_config.tg_bot_token
ei_upload_whitelist:list[str] = global_config.ei_upload_whitelist

tag_pat = re.compile(r"^#[^#]*$")

upload = on_command("投稿", rule=to_me())


# TODO: wait for website to support upload
async def download_image(url):
    async with httpx.AsyncClient() as client:
        r = await client.get(url)
        return r.content

# TODO: wait for website to support upload
async def upload_image(data):
    async with httpx.AsyncClient() as client:
        r = await client.post("https://ellye.org/api/upload", data=data)
        return r.json()


@upload.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    image_url_list = list()
    caption = list()
    caption_text = str()
    for seg in args:
        if seg.type == "image":
            image_url_list.append(seg.data["url"])
        if seg.type == "text":
            text: str = seg.data["text"]
            text.replace("＃", "#")
            text_list = text.strip().split()
            for text in text_list:
                if tag_pat.match(text):
                    caption.append(text)
                else:
                    text = text.replace("#", "")
                    if text != "":
                        caption.append(f"#{text}")
    caption = list(set(caption))
    caption_text = " ".join(caption)
    if len(caption_text) > 100:
        await upload.finish("标签过长。")
    sender_nickname = event.sender.nickname or event.sender.card or "匿名"
    sender_id = event.get_user_id()
    caption_text += f"\n投稿人：{sender_nickname}({sender_id})"
    chat_id = "@ellyesidiom_review" if sender_id not in ei_upload_whitelist else "-1001518240073"    

    reply_seg = MessageSegment.reply(event.message_id)
    if len(image_url_list) == 0:
        await upload.finish(reply_seg + "仅接受图片投稿。")
    # if needed to upload image file
    # image_contents = await asyncio.gather(*[download_image(url) for url in image_url_list])
    if len(image_url_list) > 1:
        if len(image_url_list) > 10:
            await upload.finish(reply_seg + "一次最多上传10张图片。")
        tg_url = f"https://api.telegram.org/bot{tg_bot_token}/sendMediaGroup"
        payload = {
            "chat_id": chat_id,
            "media": []
        }
        for url in image_url_list:
            if caption_text is not None:
                payload["media"].append({
                    "type": "photo",
                    "media": url,
                    "caption": caption_text
                })
                caption_text = None
            else:
                payload["media"].append({
                    "type": "photo",
                    "media": url,
                })
        async with httpx.AsyncClient() as client:
            try:
                r = await client.post(tg_url, json=payload)
            except httpx.ConnectTimeout:
                await upload.finish(reply_seg + "上传失败。")

    if len(image_url_list) == 1:
        tg_url = f"https://api.telegram.org/bot{tg_bot_token}/sendPhoto"

        payload = {
            "chat_id": chat_id,
            "photo": image_url_list[0],
            "caption": caption_text
        }

        async with httpx.AsyncClient() as client:
            try:
                r = await client.post(tg_url, json=payload)
            except httpx.ConnectTimeout:
                await upload.finish(reply_seg + "上传失败。")
    await upload.finish(reply_seg + "已投稿。")
