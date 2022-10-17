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

from .utils import global_config
from .utils import base16_to_base32, base32_to_base16
from .data_source import delete_idiom_by_id, get_ext_by_image_hash, get_review_status_by_image_hash, get_under_review_idioms, search_idiom, add_idiom, create_index, update_ocr_text, update_review_status_by_image_hash
from .data_source import count_under_review, count_reviewed
from .data_source import get_id_by_image_hash, add_tags_by_hash, check_image_hash_exists
from .storage import ei_img_storage_delete, ei_img_storage_upload, ei_img_storage_download
from .ocr import get_ocr_text_qcloud_basic, get_ocr_text_local

tg_bot_token: str = global_config.tg_bot_token
ei_upload_whitelist: list[str] = global_config.ei_upload_whitelist

tag_pat = re.compile(r"^#[^#]*$")


upload= on_command("投稿", rule=to_me())
bulk_upload = on_command("批量导入", rule=to_me())
search = on_command("查询", rule=to_me())
delete = on_command("删除", rule=to_me())
statistics = on_command("统计", rule=to_me())

add_tags = on_command("添加tag", rule=to_me())

ei_rebuild = on_command("重建ei", rule=to_me())
update_ocr = on_command("更新OCR", rule=to_me())
get_ocr_result = on_command("OCR", rule=to_me())
calculate_hash = on_command("计算", rule=to_me())

approve_idiom = on_command("通过", rule=to_me())
reject_idiom = on_command("打回", rule=to_me())
review_list = on_command("待审核列表", rule=to_me())

transport = httpx.AsyncHTTPTransport(retries=3)

client = httpx.AsyncClient(transport=transport)

async def download_image_from_qq(url):
    r = await client.get(url, timeout=10)
    return r.content


async def upload_image(matcher, image_contents: list[bytes], caption: list[str], uploader_info: dict, under_review: bool):
    print(len(image_contents))
    image_count = 0
    filename_list = list()
    large_image_list = list()
    exist_image_list = list()
    for image_content in image_contents:
        image_count += 1
        if len(image_content) > 10 * 1024 * 1024:
            large_image_list.append(image_count)
            continue
        image_hash = xxh3_64_hexdigest(image_content)
        if await check_image_hash_exists(image_hash):
            exist_image_list.append(image_count)
            continue
        file_format = filetype.guess(image_content)
        file_format = file_format.EXTENSION
        
        filename = f"{image_hash}.{file_format}"
        filename_list.append(filename)
        if not under_review:
            ocr_result = await get_ocr_text_qcloud_basic(image_content)
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
    warning_text = ""
    if len(large_image_list) > 0:
        warning_text += f"图片{large_image_list}过大，跳过上传。\n"
    if len(exist_image_list) > 0:
        warning_text += f"图片{exist_image_list}已存在，跳过上传。\n"
    if warning_text != "":
        await matcher.send(warning_text)
    return filename_list

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

async def upload_to_telegram(matcher, reply_seg, image_url_list: list[str], caption: list[str], uploader_info: dict, ei_under_review: bool, upload_ok_quote: str):
    chat_id = "@ellyesidiom_review" if ei_under_review else "-1001518240073"
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
                    await matcher.finish(reply_seg + "投稿失败，可能是Telegram端出现问题。")
            except httpx.ConnectTimeout:
                await matcher.finish(reply_seg + "上传失败，可能是遇到网络连接性问题。")

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
                    await matcher.finish(reply_seg + "投稿失败，可能是Telegram端出现问题。")
            except httpx.ConnectTimeout:
                await matcher.finish(reply_seg + "上传失败，可能是遇到网络连接性问题。")
    await matcher.finish(reply_seg + upload_ok_quote)


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

    upload_ok_quote = "上传成功，请等待审核。" if ei_under_review else "上传成功。"
    image_contents = await asyncio.gather(*[download_image_from_qq(url) for url in image_url_list])
    sender_info = {"nickname": sender_nickname,
                   "id": sender_id, "platform": "qq"}
    filename_list = await upload_image(upload, image_contents, caption_without_hash, sender_info, ei_under_review)
    image_hashes = [hash.split(".")[0] for hash in filename_list]
    upload_ok_quote += "\nID: " + " ".join(base16_to_base32(image_hashes))
    # TODO still need to fix parameters
    # await upload_to_telegram(upload, reply_seg, image_url_list, caption, sender_info, ei_under_review, upload_ok_quote)
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
        if len(result_scores) > 1 and result_scores[-1] - res["_score"] > 5:
            result_text += "后续结果相关性差距过高，放弃输出。"
            break
        limit_count += 1
        filename = f"{res['_source']['image_hash']}.{res['_source']['image_ext']}"
        image_bytes = await ei_img_storage_download(filename)
        id = await base16_to_base32(res["_source"]["image_hash"])
        result_text += MessageSegment.image(BytesIO(image_bytes))
        result_text += f"相关性：{res['_score']}\n"
        result_text += f"ID: {id}\n"
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

# import files from folder and upload
@bulk_upload.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    bulk_dir = str(args)
    if event.get_user_id() not in ei_upload_whitelist:
        await bulk_upload.finish("您没有权限使用该功能。")
    filelist = os.listdir(bulk_dir)
    for filename in filelist:
        with open(os.path.join(bulk_dir, filename), "rb") as f:
            image_content = f.read()
        await upload_image(bulk_upload, [image_content], [], {"nickname": "欧式查理", "id": "269077688", "platform": "导入"}, False)

@ei_rebuild.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await ei_rebuild.finish("您没有权限使用此命令。")
    await create_index()
    filelist = os.listdir("/home/maxesisn/botData/ei_images")
    for file in filelist:
        with open(f"/home/maxesisn/botData/ei_images/{file}", "rb") as f:
            print(f"Importing {file}")
            filename_list = await upload_image(ei_rebuild, image_contents=[f.read()], caption=[], uploader_info={"nickname": "欧式查理", "id": "269077688", "platform": "导入"}, under_review=False)
            for filename in filename_list:
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
            ocr_text = await get_ocr_text_qcloud_basic(f.read())
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
                ocr_text = await get_ocr_text_qcloud_basic(image_bytes)
                await get_ocr_result.finish()

@delete.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await delete.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await delete.finish("请输入要删除的ID。")
    image_hash = str(args)
    image_hash = await base32_to_base16(image_hash)
    id = await get_id_by_image_hash(image_hash)
    es_r = await delete_idiom_by_id(id)
    image_ext = await get_ext_by_image_hash(id)
    await ei_img_storage_delete(f"{image_hash}.{image_ext}")
    print(es_r)
    await delete.finish("已删除。")

@statistics.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    result_under_review = await count_under_review()
    result_reviewed = await count_reviewed()
    await statistics.finish(f"待审核：{result_under_review}\n已审核：{result_reviewed}")

@add_tags.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await add_tags.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await add_tags.finish("请输入要添加标签的ID。")
    args = str(args).split()
    image_hash = args[0]
    tags = args[1:]
    image_hash = await base32_to_base16(image_hash)
    print(image_hash, tags)
    es_r = await add_tags_by_hash(image_hash, tags)
    if es_r["updated"] == 1:
        await add_tags.finish("已添加标签。")
    else:
        await add_tags.finish("添加标签失败。")

@calculate_hash.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    args = str(args)
    if len(args) == 16:
        await calculate_hash.finish(await base16_to_base32(args))
    else:
        await calculate_hash.finish(await base32_to_base16(args))

@approve_idiom.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await approve_idiom.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await approve_idiom.finish("请输入要审核的ID。")
    image_hashes = str(args).split()
    fail_str = ""
    for image_hash in image_hashes:
        if len(image_hash) != 16:
            image_hash = await base32_to_base16(image_hash)
        es_r = await update_review_status_by_image_hash(image_hash, False)
        if es_r["updated"] != 1:
            fail_str += f"{image_hash} 审批失败。\n"
    if fail_str == "":
        await approve_idiom.finish("已审核。")
    else:
        await approve_idiom.finish(fail_str)

@reject_idiom.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await reject_idiom.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await reject_idiom.finish("请输入要审核的ID。")
    image_hashes = str(args).split()
    fail_str = ""
    for image_hash in image_hashes:
        if len(image_hash) != 16:
            image_hash = await base32_to_base16(image_hash)
        image_current_reviewing_status = await get_review_status_by_image_hash(image_hash)
        if image_current_reviewing_status == True:
            image_ext = await get_ext_by_image_hash(image_hash)
            id = await get_id_by_image_hash(image_hash)
            es_r = await delete_idiom_by_id(id)
            print(es_r)
            await ei_img_storage_delete(f"{image_hash}.{image_ext}")
            if es_r["result"] != "deleted":
                fail_str += f"{image_hash} 删除失败。\n"
        else:
            es_r = await update_review_status_by_image_hash(image_hash, True)
            if es_r["updated"] != 1:
                fail_str += f"{image_hash} 审批失败。\n"
    if fail_str == "":
        await reject_idiom.finish("已审核。")
    else:
        await reject_idiom.finish(fail_str)

@review_list.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await review_list.finish("您没有权限使用此命令。")
    idiom_list = await get_under_review_idioms()
    result = ""
    for idiom in idiom_list:
        result += f"hash: {idiom['_source']['image_hash']} tags:{idiom['_source']['tags']} ocr:{idiom['_source']['ocr_text']}\n"
    await review_list.finish(result)