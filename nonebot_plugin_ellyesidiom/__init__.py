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
import binascii
import httpx

from .utils import global_config
from .utils import base16_to_base32, base32_to_base16
from .data_es import search_idiom as es_search_idiom, update_ocr_text as es_update_ocr_text, add_idiom as es_add_idiom, add_tags_by_hash as es_add_tags_by_hash, delete_idiom_by_image_hash as es_delete_idiom_by_image_hash
from .data_mongo import delete_idiom_by_image_hash, get_catalogue_by_image_hash, get_comment_by_image_hash, get_ext_by_image_hash, get_idiom_by_catalogue, get_idiom_by_comment, get_review_status_by_image_hash, get_under_review_idioms, add_idiom, update_ocr_text_by_image_hash, update_review_status_by_image_hash
from .data_mongo import count_under_review, count_reviewed
from .data_mongo import add_tags_by_hash, check_image_hash_exists, check_ocr_text_exists
from .data_mongo import get_idiom_by_image_hash
from .storage import ei_img_storage_delete, ei_img_storage_upload, ei_img_storage_download
from .ocr import get_ocr_text_cloud, get_ocr_text_local
from .cat_checker import ep_alias_to_id, id_to_ep_alias, ep_alias

tg_bot_token: str = global_config.tg_bot_token
ei_upload_whitelist: list[str] = global_config.ei_upload_whitelist

tag_pat = re.compile(r"^#[^#]*$")

upload= on_command("投稿", rule=to_me())
bulk_upload = on_command("批量导入", rule=to_me())
search = on_command("查询", rule=to_me())
delete = on_command("删除", rule=to_me())
statistics = on_command("统计", rule=to_me())
edit = on_command("编辑", rule=to_me())

add_tags = on_command("添加tag", rule=to_me())

update_ocr = on_command("更新OCR", rule=to_me())
get_ocr_result = on_command("OCR", rule=to_me())
calculate_hash = on_command("计算", rule=to_me())

approve_idiom = on_command("通过", rule=to_me())
reject_idiom = on_command("打回", rule=to_me())
review_list = on_command("待审核列表", rule=to_me())
pull_image = on_command("调取", rule=to_me())

ei_help = on_command("帮助", rule=to_me())

transport = httpx.AsyncHTTPTransport(retries=3)

client = httpx.AsyncClient(transport=transport)

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
        arg_copy = arg_copy.replace("，", ",").replace("＝", "=").replace("：", ":").replace(" ", "")
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
    image_url_list, caption, extra_data = await extract_upload(args)
    if len(image_url_list) == 0 and event.reply is not None:
        reply_msg = await bot.get_msg(message_id=event.reply.message_id)
        reply_image_url_list, reply_caption, reply_extra_data = await extract_upload(reply_msg["message"])
        image_url_list = image_url_list + reply_image_url_list
        if reply_extra_data["comment"]:
            extra_data["comment"] = reply_extra_data["comment"]
        if reply_extra_data["catalogue"]:
            extra_data["catalogue"] = reply_extra_data["catalogue"]
        if reply_extra_data["no_such_cat_list"]:
            extra_data["no_such_cat_list"] = reply_extra_data["no_such_cat_list"]
    reply_seg = MessageSegment.reply(event.message_id)
    if len(image_url_list) == 0:
        await upload.finish(reply_seg + "仅接受图片投稿。")
    if extra_data["no_such_cat_list"]:
        print(extra_data["no_such_cat_list"])
        await upload.finish(reply_seg + f"没有找到分类：{', '.join(extra_data['no_such_cat_list'])}，取消上传。")
    caption = list(set(caption))
    caption_without_hash = list()
    for cap in caption:
        caption_without_hash.append(cap[1:])
    caption_text = " ".join(caption)
    caption_text = caption_text.replace("＝", "=")
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
    filename_list = await upload_image(upload, image_contents, caption_without_hash, sender_info, ei_under_review, extra_data["comment"], extra_data["catalogue"])
    image_hashes = [hash.split(".")[0] for hash in filename_list]
    upload_ok_quote += "\nID: "
    for image_hash in image_hashes:
        upload_ok_quote += f"{await base16_to_base32(image_hash)} "
    if len(image_hashes) == 0:
        await upload.finish(reply_seg + "上传失败，没有可上传的新怡闻录。")
    # TODO still need to fix parameters
    # await upload_to_telegram(upload, reply_seg, image_url_list, caption, sender_info, ei_under_review, upload_ok_quote)
    await upload.finish(reply_seg + upload_ok_quote)

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
            idiom_list = [idiom for idiom in res_idiom_list if idiom in res_com_list]
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
        await upload_image(bulk_upload, [image_content], [], {"nickname": "欧式查理", "id": "269077688", "platform": "导入"}, False, [], [])


@update_ocr.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await update_ocr.finish("您没有权限使用此命令。")
    filelist = os.listdir("/home/maxesisn/botData/ei_images")
    for file in filelist:
        filename_without_ext = file.split(".")[0]
        with open(f"/home/maxesisn/botData/ei_images/{file}", "rb") as f:
            print(f"Updating {file}")
            ocr_text = await get_ocr_text_cloud(f.read())
            await update_ocr_text_by_image_hash(filename_without_ext, ocr_text)
            await es_update_ocr_text(filename_without_ext, ocr_text)

@get_ocr_result.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await get_ocr_result.finish("您没有权限使用此命令。")
    req_hash = str(args)
    for file in os.listdir("/home/maxesisn/botData/ei_images"):
        if file.startswith(req_hash):
            with open(f"/home/maxesisn/botData/ei_images/{file}", "rb") as f:
                image_bytes = f.read()
                ocr_text = await get_ocr_text_cloud(image_bytes)
                await get_ocr_result.finish()

@delete.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await delete.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await delete.finish("请输入要删除的ID。")
    image_hashes = str(args)
    image_hashes = image_hashes.split(" ")
    result_text = ""
    for image_hash in image_hashes:
        temp_result_text = ""

        image_hash = await any_to_base16(image_hash)
        image_ext = await get_ext_by_image_hash(image_hash)
        if not image_ext:
            temp_result_text += f"未找到ID为{await base16_to_base32(image_hash)}的图片。\n"
        else:
            await ei_img_storage_delete(f"{image_hash}.{image_ext}")

        try:
            await delete_idiom_by_image_hash(image_hash)
            await es_delete_idiom_by_image_hash(image_hash)
        except IndexError:
            if temp_result_text == "":
                temp_result_text += f"未找到ID为{await base16_to_base32(image_hash)}的记录。\n"
            else:
                temp_result_text = f"未找到ID为{await base16_to_base32(image_hash)}的图片与记录。\n"
        result_text += temp_result_text
    await delete.send(result_text)
    await delete.finish("已全部删除。")

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
    image_hash = await any_to_base16(image_hash)
    print(image_hash, tags)
    await add_tags_by_hash(image_hash, tags)
    await es_add_tags_by_hash(image_hash, tags)
    await add_tags.finish("已添加标签。")


@calculate_hash.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    args = str(args)
    calculate_hash.finish(await any_to_base16(args))

@approve_idiom.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await approve_idiom.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await approve_idiom.finish("请输入要审核的ID。")
    image_hashes = str(args).split()

    for image_hash in image_hashes:
        image_hash = await any_to_base16(image_hash)
        image_ext = await get_ext_by_image_hash(image_hash)
        image_bytes = await ei_img_storage_download(f"{image_hash}.{image_ext}")
        ocr_text = await get_ocr_text_cloud(image_bytes)
        await update_ocr_text_by_image_hash(image_hash, ocr_text)
        await es_update_ocr_text(image_hash, ocr_text)
        await update_review_status_by_image_hash(image_hash, False)
    await approve_idiom.finish("已审核。")


@reject_idiom.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await reject_idiom.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await reject_idiom.finish("请输入要审核的ID。")
    image_hashes = str(args).split()
    for image_hash in image_hashes:
        image_hash = await any_to_base16(image_hash)
        image_current_reviewing_status = await get_review_status_by_image_hash(image_hash)
        if image_current_reviewing_status == True:
            image_ext = await get_ext_by_image_hash(image_hash)
            await delete_idiom_by_image_hash(image_hash)
            await es_delete_idiom_by_image_hash(image_hash)
            await ei_img_storage_delete(f"{image_hash}.{image_ext}")

        else:
            await update_review_status_by_image_hash(image_hash, True)

        await reject_idiom.finish("已审核。")


@review_list.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await review_list.finish("您没有权限使用此命令。")
    idiom_list = await get_under_review_idioms()
    result = ""
    for idiom in idiom_list:
        result += f"hash: {idiom['image_hash']} tags:{idiom['tags']} ocr:{idiom['ocr_text']} cat:{idiom['catalogue']} com:{idiom['comment']}\n"
    if result == "":
        result = "没有待审核的怡闻录。"
    await review_list.finish(result)

@pull_image.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await pull_image.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await pull_image.finish("请输入要调取的hash。")
    image_hash = str(args)
    image_hash = await any_to_base16(image_hash)
    image_ext = await get_ext_by_image_hash(image_hash)
    image = await ei_img_storage_download(f"{image_hash}.{image_ext}")
    await pull_image.finish(MessageSegment.image(image))

@ei_help.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    ei_help_msg = """0. 首先at怡闻录bot
1. 投稿：投稿 标签1 标签2 cat=分类1,分类2 com=备注1,备注2 [图片1] [图片2]
2. 查询：查询 标签1 标签2 cat=分类1,分类2 com=备注1,备注2 [图片1] [图片2]
剩下的都是管理员命令，不告诉你
"""
    ep_alias_text = ""
    for k, v in ep_alias.items():
        ep_alias_text += f"{v[0]}: {','.join(v)}\n"
    ei_help_msg += f"[注1]可用分类：\n{ep_alias_text}"
    await ei_help.finish(ei_help_msg)

@edit.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await edit.finish("您没有权限使用此命令。")
    if len(args) == 0:
        await edit.finish("请输入要编辑的hash。")
    image_hash = str(args)
    