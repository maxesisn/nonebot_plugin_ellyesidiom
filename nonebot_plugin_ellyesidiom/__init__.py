import asyncio
from io import BytesIO
import os
from nonebot import on_command
from nonebot.adapters.onebot.v11 import Bot, Event, Message, MessageSegment
from nonebot.params import CommandArg
from nonebot.rule import to_me
import re
from xxhash import xxh3_64_hexdigest
import filetype
from nonebot.log import logger

import httpx

from .config import global_config
from .data_source import delete_idiom_by_id, search_idiom, add_idiom, create_index, update_ocr_text
from .storage import ei_img_storage_delete, ei_img_storage_upload, ei_img_storage_download
from .ocr import get_ocr_text_qcloud, get_ocr_text_local

tg_bot_token: str = global_config.tg_bot_token
ei_upload_whitelist: list[str] = global_config.ei_upload_whitelist

tag_pat = re.compile(r"^#[^#]*$")

upload = on_command("投稿", rule=to_me())
search = on_command("查询", rule=to_me())
delete = on_command("删除", rule=to_me())

ei_import = on_command("导入", rule=to_me())
update_ocr = on_command("更新OCR", rule=to_me())
get_ocr_result = on_command("OCR", rule=to_me())

transport = httpx.AsyncHTTPTransport(retries=3)

client = httpx.AsyncClient(transport=transport)

async def download_image_from_qq(url):
    r = await client.get(url, timeout=10)
    return r.content


async def upload_image(image_contents: list[bytes], caption: list[str], uploader_info: dict, under_review: bool):
    print(len(image_contents))
    for image_content in image_contents:
        if len(image_content) > 10 * 1024 * 1024:
            continue
        file_format = filetype.guess(image_content)
        file_format = file_format.EXTENSION
        image_hash = xxh3_64_hexdigest(image_content)
        filename = f"{image_hash}.{file_format}"
        if not under_review:
            ocr_result = await get_ocr_text_qcloud(image_content)
        else:
            ocr_result = await get_ocr_text_local(image_content)
        await ei_img_storage_upload(filename, image_content)
        # save bytes to local file
        with open(os.path.join(global_config.cache_dir, filename), "wb") as f:
            f.write(image_content)
        await add_idiom(tags=caption, image_hash=image_hash, image_ext=file_format, ocr_text=ocr_result, uploader_info=uploader_info, under_review=under_review)
        if caption:
            logger.info(f"Uploaded {image_hash} with tags {caption}")
        else:
            logger.info(f"Uploaded {image_hash} with ocr text {ocr_result}")
    return filename

async def extract_upload(args):
    image_url_list = list()
    caption = list()
    for seg in args:
        if isinstance(seg, MessageSegment):
            seg_type = seg.type
            seg_data = seg.data
        else:
            seg_type = seg["type"]
            seg_data = seg["data"]
        if seg_type == "image":
            image_url_list.append(seg_data["url"])
        if seg_type == "text":
            text: str = seg_data["text"]
            text.replace("＃", "#")
            text_list = text.strip().split()
            for text in text_list:
                if tag_pat.match(text):
                    caption.append(text)
                else:
                    text = text.replace("#", "")
                    if text != "":
                        caption.append(f"#{text}")
    return image_url_list, caption


@upload.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    caption_text = str()
    image_url_list, caption = await extract_upload(args)
    if len(image_url_list) == 0 and event.reply is not None:
        reply_msg = await bot.get_msg(message_id=event.reply.message_id)
        reply_image_url_list, reply_caption = await extract_upload(reply_msg["message"])
        image_url_list = image_url_list + reply_image_url_list
    reply_seg = MessageSegment.reply(event.message_id)
    if len(image_url_list) == 0:
        await upload.finish(reply_seg + "仅接受图片投稿。")
    caption = list(set(caption))
    caption_without_hash = list()
    for cap in caption:
        caption_without_hash.append(cap[1:])
    caption_text = " ".join(caption)
    if len(caption_text) > 100:
        await upload.finish("标签过长。")
    sender_nickname = event.sender.nickname or event.sender.card or "匿名"
    sender_id = event.get_user_id()
    caption_text += f"\n投稿人：{sender_nickname}({sender_id})"
    ei_under_review = False if sender_id in ei_upload_whitelist else True
    chat_id = "@ellyesidiom_review" if not ei_under_review else "-1001518240073"
    upload_ok_quote = "上传成功。" if not ei_under_review else "上传成功，请等待审核。"
    
    image_contents = await asyncio.gather(*[download_image_from_qq(url) for url in image_url_list])
    sender_info = {"nickname": sender_nickname,
                   "id": sender_id, "platform": "qq"}
    await upload_image(image_contents, caption_without_hash, sender_info, ei_under_review)

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
        async with httpx.AsyncClient(transport=transport) as client:
            try:
                r = await client.post(tg_url, json=payload, timeout=10)
                if not r.json()["ok"]:
                    await upload.finish(reply_seg + "投稿失败，可能是Telegram端出现问题。")
            except httpx.ConnectTimeout:
                await upload.finish(reply_seg + "上传失败，可能是遇到网络连接性问题。")

    if len(image_url_list) == 1:
        tg_url = f"https://api.telegram.org/bot{tg_bot_token}/sendPhoto"

        payload = {
            "chat_id": chat_id,
            "photo": image_url_list[0],
            "caption": caption_text
        }

        async with httpx.AsyncClient(transport=transport) as client:
            try:
                r = await client.post(tg_url, json=payload, timeout=10)
                if not r.json()["ok"]:
                    await upload.finish(reply_seg + "投稿失败，可能是Telegram端出现问题。")
            except httpx.ConnectTimeout:
                await upload.finish(reply_seg + "上传失败，可能是遇到网络连接性问题。")
    await upload.finish(reply_seg + upload_ok_quote)


async def get_idiom_result(keyword: str, limit: int):
    limit_count = 0
    result = await search_idiom(keyword)

    if len(result["hits"]["total"]) == 0:
        return None, 0
    result_text = ""
    result_hits = result["hits"]["hits"]
    result_scores = [0]
    for res in result_hits:
        if res["_score"] < 1:
            continue
        if result_scores[-1] - res["_score"] > 5 and len(result_scores) > 1:
            result_text += "后续结果相关性差距过高，放弃输出。"
            break
        limit_count += 1
        filename = f"{res['_source']['image_hash']}.{res['_source']['image_ext']}"
        image_bytes = await ei_img_storage_download(filename)
        result_text += MessageSegment.image(BytesIO(image_bytes))
        result_text += f"相关性：{res['_score']}\n"
        if len(res["_source"]["tags"]) > 0:
            result_text += f"标签：{' '.join(res['_source']['tags'])}\n"
        else:
            result_text += "来源：文字OCR\n"
        if limit_count >= limit:
            break
        result_scores.append(res["_score"])
    return result_text, limit_count


@search.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if len(args) == 0:
        await search.finish("请输入查询关键词。")
    keyword = str(args)
    keyword.replace("#", "")
    if keyword == "":
        await search.finish("请输入查询关键词。")
    result_str, count = await get_idiom_result(keyword, 5)
    if result_str == "":
        await search.finish("未找到相关结果。")
    await search.finish(result_str)


@ei_import.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await ei_import.finish("您没有权限使用此命令。")
    await create_index()
    filelist = os.listdir("/home/maxesisn/botData/ei_images")
    for file in filelist:
        with open(f"/home/maxesisn/botData/ei_images/{file}", "rb") as f:
            print(f"Importing {file}")
            filename = await upload_image(image_contents=[f.read()], caption=[], uploader_info={"nickname": "欧式查理", "id": "269077688", "platform": "导入"}, under_review=False)
            os.rename(f"/home/maxesisn/botData/ei_images/{file}", f"/home/maxesisn/botData/ei_images/{filename}")

@update_ocr.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await update_ocr.finish("您没有权限使用此命令。")
    filelist = os.listdir("/home/maxesisn/botData/ei_images")
    for file in filelist:
        filename_without_ext = file.split(".")[0]
        with open(f"/home/maxesisn/botData/ei_images/{file}", "rb") as f:
            print(f"Updating {file}")
            ocr_text = await get_ocr_text_qcloud(f.read())
            await update_ocr_text(filename_without_ext, ocr_text)

@get_ocr_result.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await get_ocr_result.finish("您没有权限使用此命令。")
    req_hash = str(args)
    for file in os.listdir("/home/maxesisn/botData/ei_images"):
        if file.startswith(req_hash):
            with open(f"/home/maxesisn/botData/ei_images/{file}", "rb") as f:
                image_bytes = f.read()
                ocr_text = await get_ocr_text_qcloud(image_bytes)
                await get_ocr_result.finish()
            
@delete.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await delete.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await delete.finish("请输入要删除的ID。")
    id = str(args)
    es_r = await delete_idiom_by_id(id)
    print(es_r)
    await delete.finish("已删除。")
