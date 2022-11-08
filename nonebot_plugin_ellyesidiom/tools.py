import base64
import math
import os
import re
from io import BytesIO

import binascii
import filetype
import httpx
from xxhash import xxh3_64_hexdigest
from nonebot.adapters.onebot.v11 import MessageSegment
from nonebot.log import logger

from .data_es import search_idiom as es_search_idiom, add_idiom as es_add_idiom
from .data_mongo import get_catalogue_by_image_hash, get_comment_by_image_hash, get_idiom_by_catalogue, get_idiom_by_comment, add_idiom
from .data_mongo import check_image_hash_exists, check_ocr_text_exists
from .data_mongo import get_idiom_by_image_hash
from .storage import ei_img_storage_upload, ei_img_storage_download
from .ocr import get_ocr_text_cloud, get_ocr_text_local
from .cat_checker import ep_alias_to_id, id_to_ep_alias


from .data_mongo import check_image_hash_exists

from .consts import global_config, shanghai_tz


transport = httpx.AsyncHTTPTransport(retries=3)
client = httpx.AsyncClient(transport=transport)
tg_bot_token: str = global_config.tg_bot_token
tag_pat = re.compile(r"^#[^#]*$")

# base16 to base32


async def base16_to_base32(base16: str) -> str:
    return base64.b32encode(bytearray.fromhex(base16)).decode('utf-8').replace('=', '')


async def base32_to_base16(base32: str) -> str:
    pad_length = math.ceil(len(base32) / 8) * 8 - len(base32)
    base32 = base32 + '=' * pad_length
    return base64.b32decode(base32).hex()


async def download_image_from_qq(url):
    r = await client.get(url, timeout=10)
    return r.content


async def any_to_base16(bxx_str):
    try:
        base16 = await base32_to_base16(bxx_str)
    except binascii.Error:
        base16 = bxx_str
    return base16


async def ei_argparser(args: list[str]) -> dict:
    include_comment_list = list()
    exclude_comment_list = list()
    include_catalogue_list = list()
    exclude_catalogue_list = list()
    ready_to_removed = list()
    for arg in args:
        if isinstance(arg, MessageSegment):
            arg_type = arg.type
            arg_data = arg.data
        else:
            arg_type = arg["type"]
            arg_data = arg["data"]
        arg_copy = arg
        arg_copy = arg_copy.replace("，", ",").replace(
            "＝", "=").replace("：", ":").replace(" ", "")
        match arg_copy[:4]:
            case "com=" | "com:":
                include_comment_list.append(arg_copy[4:])
                ready_to_removed.append(arg)
            case "com!":
                if arg_copy[4] == "=":
                    exclude_comment_list.append(arg_copy[5:])
                    ready_to_removed.append(arg)
            case "cat=" | "cat:":
                include_catalogue_list.append(arg_copy[4:])
                ready_to_removed.append(arg)
            case "cat!":
                if arg_copy[4] == "=":
                    exclude_catalogue_list.append(arg_copy[5:])
                    ready_to_removed.append(arg)
    for arg in ready_to_removed:
        args.remove(arg)

    return {
        "processed_args": args,
        "include_comment_list": include_comment_list,
        "exclude_comment_list": exclude_comment_list,
        "include_catalogue_list": include_catalogue_list,
        "exclude_catalogue_list": exclude_catalogue_list
    }


async def upload_image(matcher, image_contents: list[bytes], caption: list[str], uploader_info: dict, under_review: bool, comment: list[str], catalogue: list[str]):
    image_count = 0
    filename_list = list()
    large_image_list = list()
    exist_image_list = list()
    no_ocr_content_list = list()
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
            ocr_result = await get_ocr_text_cloud(image_content)
        else:
            ocr_result = await get_ocr_text_local(image_content)
        if ocr_result is None:
            no_ocr_content_list.append(image_count)
            continue
        await ei_img_storage_upload(filename, image_content)
        # save bytes to local file
        with open(os.path.join(global_config.cache_dir, filename), "wb") as f:
            f.write(image_content)
        await add_idiom(tags=caption, image_hash=image_hash, image_ext=file_format, ocr_text=ocr_result, uploader_info=uploader_info, under_review=under_review, comment=comment, catalogue=catalogue)
        await es_add_idiom(tags=caption, image_hash=image_hash, ocr_text=ocr_result, under_review=under_review)
        if caption:
            logger.info(f"Uploaded {image_hash} with tags {caption}")
        else:
            logger.info(f"Uploaded {image_hash} with ocr text {ocr_result}")
    warning_text = ""
    if len(large_image_list) > 0:
        warning_text += f"图片{large_image_list}过大，跳过上传。\n"
    if len(exist_image_list) > 0 or print(await check_ocr_text_exists(ocr_result)):
        warning_text += f"图片{exist_image_list}已存在，跳过上传。\n"
    if len(no_ocr_content_list) > 0 and not caption:
        warning_text += f"图片{no_ocr_content_list}无标签且未识别到文字，跳过上传。\n"
    if warning_text != "":
        await matcher.send(warning_text)
    return filename_list


async def extract_upload(args):
    extra_data = dict()
    extra_data["comment"] = list()
    extra_data["catalogue"] = list()
    image_url_list = list()
    no_such_cat_list = list()
    caption = list()
    # parsed_args = await ei_argparser(args)
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
            text_block: str = seg_data["text"]
            text_block = text_block.strip().split()
            for text in text_block:
                if text.startswith("com="):
                    extra_data["comment"].append(text[4:].strip())
                    continue
                if text.startswith("cat="):
                    cat = text[4:].strip().split(",")
                    for c in cat:
                        c_res = await ep_alias_to_id(c)
                        if c_res is not None:
                            extra_data["catalogue"].append(c_res)
                        else:
                            no_such_cat_list.append(c)
                    continue
                text.replace("＃", "#")
                if tag_pat.match(text):
                    caption.append(text)
                else:
                    text = text.replace("#", "")
                    if text != "":
                        caption.append(f"#{text}")
    extra_data["no_such_cat_list"] = no_such_cat_list
    if not extra_data["catalogue"]:
        extra_data["catalogue"].append(await ep_alias_to_id("怡宝"))
    return image_url_list, caption, extra_data


async def upload_to_telegram(matcher, reply_seg, image_url_list: list[str], caption: list[str], uploader_info: dict, ei_under_review: bool, upload_ok_quote: str):
    chat_id = "@ellyesidiom_review" if ei_under_review else "-1001518240073"
    if len(image_url_list) > 1:
        if len(image_url_list) > 10:
            await matcher.finish(reply_seg + "一次最多上传10张图片。")
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


def common_member(a, b):
    a_set = set(a)
    b_set = set(b)
    if (a_set & b_set):
        return True
    else:
        return False


async def get_idiom_result(keyword: str, limit: int):
    keyword_list = keyword.split(" ")
    cat_id_list = list()
    com_list = list()
    for kl in keyword_list:
        kl.replace("＝", "=")
        if kl.startswith("cat="):
            for cat in kl[4:].split(","):
                cat_id_list.append(await ep_alias_to_id(cat))
            keyword = keyword.replace(kl, "")
        elif kl.startswith("com="):
            com_list.append(kl[4:])
            keyword = keyword.replace(kl, "")
    limit_count = 0
    if keyword.strip() == "":
        # 无关键词，按照分类/备注搜索
        result_text = ""
        if cat_id_list and com_list:
            res_idiom_list = await get_idiom_by_catalogue(cat_id_list)
            res_idiom_list = list(res_idiom_list)
            res_com_list = await get_idiom_by_comment(com_list)
            res_com_list = list(res_com_list)
            idiom_list = [
                idiom for idiom in res_idiom_list if idiom in res_com_list]
        elif cat_id_list:
            idiom_list = await get_idiom_by_catalogue(cat_id_list)
            idiom_list = list(idiom_list)
        elif com_list:
            res_com_list = await get_idiom_by_comment(com_list)
            res_com_list = list(res_com_list)
            idiom_list.extend(res_com_list)

        for res in idiom_list:
            filename = f"{res['image_hash']}.{res['image_ext']}"
            image_bytes = await ei_img_storage_download(filename)
            id = await base16_to_base32(res['image_hash'])
            result_text += MessageSegment.image(BytesIO(image_bytes))
            result_text += f"ID: {id}\n"
            if len(res["tags"]) > 0:
                result_text += f"标签：{' '.join(res['tags'])}\n"
            if len(res["catalogue"]) > 0:
                cat_name = list()
                for cat in res["catalogue"]:
                    cat_name.append(await id_to_ep_alias(cat))
                result_text += f"分类：{' '.join(cat_name)}\n"
            if len(res["comment"]) > 0:
                result_text += f"备注：{' '.join(res['comment'])}\n"
            limit_count += 1
            if limit_count >= limit:
                break
        return result_text, limit_count

    else:
        # 按照关键词搜索并筛选
        result = await es_search_idiom(keyword)
        if len(result["hits"]["total"]) == 0:
            return None, 0
        result_text = ""
        result_hits = result["hits"]["hits"]
        result_scores = [0]
        for res in result_hits:
            if res["_score"] < 1:
                continue
            if com_list:
                mg_res = await get_comment_by_image_hash(res["_source"]["image_hash"])
                if not common_member(com_list, mg_res):
                    continue
            if cat_id_list:
                mg_res = await get_catalogue_by_image_hash(res["_source"]["image_hash"])
                if not common_member(cat_id_list, mg_res):
                    continue
            # if len(result_scores) > 1 and result_scores[-1] - res["_score"] > 5:
            #     result_text += "后续结果相关性差距过高，放弃输出。"
            #     break
            limit_count += 1
            mongo_res = await get_idiom_by_image_hash(res['_source']['image_hash'])
            if mongo_res["under_review"]:
                continue
            filename = f"{mongo_res['image_hash']}.{mongo_res['image_ext']}"
            image_bytes = await ei_img_storage_download(filename)
            id = await base16_to_base32(mongo_res['image_hash'])
            result_text += MessageSegment.image(BytesIO(image_bytes))
            result_text += f"相关性：{res['_score']}\n"
            result_text += f"ID: {id}\n"
            if len(res["_source"]["tags"]) > 0:
                result_text += f"标签：{' '.join(mongo_res['tags'])}\n"
            else:
                result_text += "来源：文字OCR\n"
            if limit_count >= limit:
                break
            result_scores.append(res["_score"])
        return result_text, limit_count
