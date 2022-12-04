import os
import random
import re
from io import BytesIO

import filetype
import httpx
from xxhash import xxh3_64_hexdigest
from nonebot import get_bots
from nonebot.adapters.onebot.v11 import Message, MessageSegment
from nonebot.adapters.onebot.v11.exception import ActionFailed
from itertools import tee, islice, chain
from nonebot.log import logger
from PIL import Image, ImageDraw
import base64

from .data_es import find_similar_idioms_by_ocr_text, search_idiom as es_search_idiom, add_idiom as es_add_idiom
from .data_mongo import get_catalogue_by_image_hash, get_comment_by_image_hash, get_idiom_by_catalogue, get_idiom_by_comment, add_idiom, get_ocr_text_by_image_hash
from .data_mongo import check_image_hash_exists, check_ocr_text_exists
from .data_mongo import get_idiom_by_image_hash
from .data_mongo import get_full_hash_by_prefix
from .data_mongo import get_gm_info, set_gm_info
from .storage import ei_img_storage_upload, ei_img_storage_download
from .ocr import get_ocr_text_cloud, get_ocr_text_local
from .cat_checker import ep_alias_to_id, id_to_ep_alias
from .exceptions import HashPrefixNotFoundError, HashPrefixConflictError


from .data_mongo import check_image_hash_exists

from .consts import global_config, shanghai_tz

ellye_gid = global_config.ellye_gid


transport = httpx.AsyncHTTPTransport(retries=3)
client = httpx.AsyncClient(transport=transport)
tg_bot_token: str = global_config.tg_bot_token
tag_pat = re.compile(r"^#[^#]*$")


async def hash_shortener(base16_str: str) -> str:
    return base16_str[:6].upper()

async def hash_extender(base16_str: str, gid: str) -> str:
    base16_str = base16_str.lower()
    hash_list = await get_full_hash_by_prefix(base16_str)
    if hash_list is None:
        raise HashPrefixNotFoundError(base16_str, gid)
    if len(hash_list) == 1:
        return hash_list[0]
    else:
        raise HashPrefixConflictError(base16_str, hash_list, gid)

async def check_dedup(image_hashes: list[str], upload_ok_quote: str) -> str:
    for image_hash in image_hashes:
        final_ocr_text = await get_ocr_text_by_image_hash(image_hash)
        dedup_result = await find_similar_idioms_by_ocr_text(final_ocr_text)
        try:
            duplicate_idiom_hash = dedup_result["hits"]["hits"][0]["_source"]["image_hash"]
            score = dedup_result["hits"]["hits"][0]["_score"]
            if image_hash == duplicate_idiom_hash:
                duplicate_idiom_hash = dedup_result["hits"]["hits"][1]["_source"]["image_hash"]
                score = dedup_result["hits"]["hits"][1]["_score"]
        except IndexError:
            return upload_ok_quote
        if score > 8:
            duplicate_idiom_id = await hash_shortener(duplicate_idiom_hash)
            upload_ok_quote += f"\n警告：{await hash_shortener(image_hash)} 似乎与已有怡言 {duplicate_idiom_id} 重复，分数：{score}。"
    return upload_ok_quote

async def download_image_from_qq(url):
    r = await client.get(url, timeout=10)
    return r.content

# solution from https://stackoverflow.com/questions/1011938/loop-that-also-accesses-previous-and-next-values

def previous_and_current_and_next_and_nextnext(some_iterable):
    prevs, items, nexts, nextnexts = tee(some_iterable, 4)
    prevs = chain([None], prevs)
    nexts = chain(islice(nexts, 1, None), [None])
    nextnexts = chain(islice(nextnexts, 2, None), [None, None])
    return zip(prevs, items, nexts, nextnexts)


async def ei_argparser(message: Message | list) -> dict:
    arg_template = {
        "cat": ["cat", "cats", "category", "categories", "分类"],
        "tag": ["tag", "tags", "标签"],
        "com": ["com", "comment", "comments", "注释", "备注"],
    }

    pure_text = list()

    if isinstance(message, Message):
        for seg in message:
            if seg.type == "text":
                pure_text.append(seg.data["text"]
                .replace("＃", "#")
                .replace("＝", "=")
                .replace("，", ",")
                .replace(", ", ",")
            )
    else:
        if isinstance(message[0], str):
            pure_text = message
        else:
            for seg in message:
                if seg["type"] == "text":
                    pure_text.append(seg["data"]["text"]
                    .replace("＃", "#")
                    .replace("＝", "=")
                    .replace("，", ",")
                    .replace(", ", ",")
                )
        
    
    pure_text = " ".join(pure_text).split()

    arg_result = dict()
    for arg_type, _ in arg_template.items():
        arg_result[arg_type] = list()

    iter_pure_text = previous_and_current_and_next_and_nextnext(pure_text)
    
    def process_argv(argv:str) -> list:
        if "=" in argv:
            argv = argv.split("=", 1)[1]
        if "," in argv:
            return argv.split(",")
        else:
            return [argv]

    for previous_arg, arg, next_arg, nextnext_arg in iter_pure_text:
        print(f"1.current result: {arg_result}")
        print(f"2.previous_arg: {previous_arg}, arg: {arg}, next_arg: {next_arg}, nextnext_arg: {nextnext_arg}")
        is_arg = False
        for k, v in arg_template.items():
            print(f"arg: {arg}, v: {v}")
            if arg.startswith(tuple(v)): # 看着像是个参数表达式
                try:
                    if "=" not in arg:  # 不构成完整的参数表达式 a=b
                        print(f"不构成完整的参数表达式 {arg}")
                        if next_arg and next_arg.startswith("="): # 和下一个参数组合起来可能是完整的参数表达式
                            if "=" == next_arg: # 淦其实只有一个等于号，但可能和下下个参数组合起来是完整的参数表达式
                                if nextnext_arg : # 有下下个参数
                                    arg = arg + next_arg + nextnext_arg # 肯定是完整的参数表达式了
                                    arg_result[k].extend(process_argv(arg))
                                    is_arg = True
                                    next(iter_pure_text) # 跳到下下下个参数
                                    next(iter_pure_text)
                                    break
                            else: # 肯定是完整的参数表达式了
                                arg = arg + next_arg 
                                arg_result[k].extend(process_argv(arg))
                                is_arg = True
                                next(iter_pure_text) # 跳到下下个参数
                                break
                        else:
                            raise IndexError # 没有下一个参数，肯定不是完整的参数表达式
                    elif arg.endswith("="): # 有等于号，但是等于号在最后，肯定不是完整的参数表达式
                        print(f"有等于号，但是等于号在最后，肯定不是完整的参数表达式 {arg}")
                        if next_arg: # 有下一个参数
                            arg = arg + next_arg
                            arg_result[k].extend(process_argv(arg))
                            is_arg = True
                            next(iter_pure_text)
                            break
                    elif arg.startswith("="):
                        raise IndexError # 等于号在最前面，肯定不是完整的参数表达式
                    else: # 构成完整的参数表达式 a=b
                        print(f"构成完整的参数表达式 {arg}")
                        argv = arg.split("=", 1)[1]
                        if not argv: # 但是没有参数值
                            arg_result[k].extend(process_argv(arg))
                        else:
                            arg_result[k].extend(process_argv(argv))
                        is_arg = True
                    break    
                except IndexError: # 误会了，不是参数表达式
                    arg_result[k].extend(process_argv(arg))
                    is_arg = False
                    break
            else: # 不是参数表达式
                print(f"对于{v=} 不是参数表达式 {arg}")
            
        if not is_arg:
            arg_result["tag"].extend(process_argv(arg))
        is_arg = False

    cat_id_list = list()
    no_cat_id_list = list()

    if not arg_result["cat"]:
        arg_result["cat"] = ["怡宝"]

    for cat in arg_result["cat"]:
        c_res = await ep_alias_to_id(cat)
        if c_res:
            cat_id_list.append(c_res)
        else:
            no_cat_id_list.append(cat)
    
    arg_result["cat"] = cat_id_list
    arg_result["no_cat"] = no_cat_id_list

    # deduplicate
    for k, v in arg_result.items():
        arg_result[k] = list(set(v))
    print(f"3.result: {arg_result}")

    return arg_result

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
    image_url_list = list()

    for seg in args:
        if isinstance(seg, MessageSegment):
            seg_type = seg.type
            seg_data = seg.data
        else:
            seg_type = seg["type"]
            seg_data = seg["data"]
        
        if seg_type == "image":
            image_url_list.append(seg_data["url"])

    parsed_args = await ei_argparser(args)
    extra_data["comment"] = parsed_args["com"]
    extra_data["catalogue"] = parsed_args["cat"]
    caption = parsed_args["tag"]
    extra_data["no_such_cat_list"] = parsed_args["no_cat"]

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
    args = await ei_argparser(keyword_list)
    cat_id_list = args["cat"]
    com_list = args["com"]
    
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
            idiom_list = await get_idiom_by_comment(com_list)
            idiom_list = list(idiom_list)

        for res in idiom_list:
            filename = f"{res['image_hash']}.{res['image_ext']}"
            image_bytes = await ei_img_storage_download(filename)
            img_id = await hash_shortener(res['image_hash'])
            result_text += MessageSegment.image(BytesIO(image_bytes))
            result_text += f"ID: {img_id}\n"
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
            img_id = await hash_shortener(mongo_res['image_hash'])
            result_text += MessageSegment.image(BytesIO(image_bytes))
            result_text += f"相关性：{res['_score']}\n"
            result_text += f"ID: {img_id}\n"
            if len(res["_source"]["tags"]) > 0:
                result_text += f"标签：{' '.join(mongo_res['tags'])}\n"
            else:
                result_text += "来源：文字OCR\n"
            if limit_count >= limit:
                break
            result_scores.append(res["_score"])
        return result_text, limit_count

async def message_striper(msg: Message):
    if len(msg) == 0:
        return ""
    if msg[-1].type == "text":
        msg[-1].data["text"] = msg[-1].data["text"].strip()
    return msg

async def message_filter(msg: Message):

    def ri() -> int:
        return random.randint(0, 8)

    def rc() -> int:
        return random.randint(0, 255)
    
    
    async def draw_point(img: str) -> str:
        img = base64.b64decode(img.replace("base64://", ""))
        img = Image.open(BytesIO(img))
        draw = ImageDraw.Draw(img)
        draw.point([(ri(), ri()), (ri(), ri()), (ri(), ri()), (ri(), ri())], fill=(rc(), rc(), rc()))
        draw.point([(img.width - ri(), img.height - ri()), (img.width - ri(), img.height - ri()),
                    (img.width - ri(), img.height - ri()), (img.width - ri(), img.height - ri())], fill=(rc(), rc(), rc()))
        img_bytes = BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes = img_bytes.getvalue()
        img_bytes = base64.b64encode(img_bytes)
        return f"base64://{img_bytes.decode()}"
    

    new_msg = Message()
    new_msg.append(MessageSegment.text("结果中有敏感图片，已进行处理\n\n"))
    for seg in msg:
        if seg.type == "image":
            seg.data["file"] = await draw_point(seg.data["file"])
        new_msg.append(seg)

    return new_msg

async def get_card_with_cache(id):
    bot = get_bots().values().__iter__().__next__()
    id = str(id)
    card = await get_gm_info(id)
    if card is None:
        try:
            giver_info: dict = await bot.get_group_member_info(group_id=int(ellye_gid[0]), user_id=id)
            card: str = giver_info["card"] or giver_info["nickname"] or giver_info["user_id"]
            await set_gm_info(id, card)
            logger.info(f"{id}'s card is {card}, cached it")
        except ActionFailed:
            card = id
    else:
        logger.debug(f"read user card from cache succeed: {card}")
    return card