import asyncio
import os
from nonebot import on_command, on_notice
from nonebot.adapters.onebot.v11 import Bot, Event, Message, MessageSegment, PokeNotifyEvent
from nonebot.params import CommandArg
from nonebot.rule import to_me
from nonebot.adapters.onebot.v11.exception import ActionFailed


from .tools import download_image_from_qq, extract_upload, global_config, upload_image
from .tools import get_idiom_result
from .tools import message_striper, message_filter
from .tools import hash_extender, hash_shortener
from .tools import get_card_with_cache
from .tools import ei_argparser
from .tools import check_dedup
from .data_es import update_ocr_text as es_update_ocr_text, add_tags_by_hash as es_add_tags_by_hash, delete_idiom_by_image_hash as es_delete_idiom_by_image_hash
from .data_mongo import delete_idiom_by_image_hash, edit_catalogue_by_image_hash, edit_comment_by_image_hash, edit_tags_by_hash, get_ext_by_image_hash, get_review_status_by_image_hash, get_under_review_idioms, update_ocr_text_by_image_hash, update_review_status_by_image_hash
from .data_mongo import count_under_review, count_reviewed
from .data_mongo import add_tags_by_hash
from .data_mongo import get_uploader_rank
from .data_mongo import get_random_idiom
from .data_redis import get_ratelimited, set_ratelimited
from .storage import ei_img_storage_delete, ei_img_storage_download
from .ocr import get_ocr_text_cloud
from .cat_checker import ep_alias
from .eh_server import *
from .consts import tips_no_permission

ei_upload_whitelist: list[str] = global_config.ei_upload_whitelist
ellye_gid = global_config.ellye_gid


def _poke_checker(event: PokeNotifyEvent) -> bool:
    print(f"{event.target_id=} {event.self_id=}")
    return event.target_id == event.self_id


upload = on_command("投稿", rule=to_me())
bulk_upload = on_command("批量导入", rule=to_me())
search = on_command("查询", rule=to_me())
delete = on_command("删除", rule=to_me())
statistics = on_command("统计", rule=to_me())
rank = on_command("排行", rule=to_me())
edit = on_command("编辑", rule=to_me())

random_idiom_poke = on_notice(rule=_poke_checker)
random_idiom_command = on_command("每日怡言", rule=to_me())

add_tags = on_command("添加tag", rule=to_me())

update_ocr = on_command("更新OCR", rule=to_me())
get_ocr_result = on_command("OCR", rule=to_me())
calculate_hash = on_command("计算", rule=to_me())

approve_idiom = on_command("通过", rule=to_me())
reject_idiom = on_command("打回", rule=to_me())
review_list = on_command("待审核列表", rule=to_me())
pull_image = on_command("调取", rule=to_me())

ei_help = on_command("帮助", rule=to_me())

test_ap = on_command("测试ap", rule=to_me())


@upload.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
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
        upload_ok_quote += f"{await hash_shortener(image_hash)} "
    if len(image_hashes) == 0:
        await upload.finish(reply_seg + "上传失败，没有可上传的新怡闻录。")
    # TODO still need to fix parameters
    # await upload_to_telegram(upload, reply_seg, image_url_list, caption, sender_info, ei_under_review, upload_ok_quote)

    upload_ok_quote = await check_dedup(image_hashes, upload_ok_quote)

    await upload.finish(reply_seg + upload_ok_quote)


@search.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if len(args) == 0:
        await search.finish("请输入查询关键词。")
    keyword = str(args)
    keyword = keyword.replace("#", "")
    if keyword == "":
        await search.finish("请输入查询关键词。")
    result_msg, count = await get_idiom_result(keyword, 5)
    result_msg = await message_striper(result_msg)
    if not result_msg:
        await search.finish("未找到相关结果。")
    try:
        await search.finish(result_msg)
    except ActionFailed:
        try:
            result_msg = await message_filter(result_msg)
            await search.finish(result_msg)
        except ActionFailed:
            try:
                await search.finish("结果中有过多敏感词，不予发送。")
            except ActionFailed:
                await search.finish()


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
        await update_ocr.finish(tips_no_permission)
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
        await get_ocr_result.finish(tips_no_permission)
    req_hash = str(args)
    for file in os.listdir("/home/maxesisn/botData/ei_images"):
        if file.startswith(req_hash):
            with open(f"/home/maxesisn/botData/ei_images/{file}", "rb") as f:
                image_bytes = f.read()
                ocr_text = await get_ocr_text_cloud(image_bytes)
                print(ocr_text)
                await get_ocr_result.finish()


@delete.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await delete.finish(tips_no_permission)
    if len(args) == 0:
        await delete.finish("请输入要删除的ID。")
    image_hashes = str(args)
    image_hashes = image_hashes.split(" ")
    result_text = ""
    for image_hash in image_hashes:
        temp_result_text = ""

        image_hash = await hash_extender(image_hash, event.group_id)
        image_ext = await get_ext_by_image_hash(image_hash)
        if not image_ext:
            temp_result_text += f"未找到ID为{await hash_shortener(image_hash)}的图片。\n"
        else:
            await ei_img_storage_delete(f"{image_hash}.{image_ext}")

        try:
            await delete_idiom_by_image_hash(image_hash)
            await es_delete_idiom_by_image_hash(image_hash)
        except IndexError:
            if temp_result_text == "":
                temp_result_text += f"未找到ID为{await hash_shortener(image_hash)}的记录。\n"
            else:
                temp_result_text = f"未找到ID为{await hash_shortener(image_hash)}的图片与记录。\n"
        result_text += temp_result_text
    await delete.send(result_text)
    await delete.finish("已全部删除。")


@statistics.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    result_under_review = await count_under_review()
    result_reviewed = await count_reviewed()
    await statistics.finish(f"待审核：{result_under_review}\n已审核：{result_reviewed}")


@rank.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    rank_result = await get_uploader_rank()
    msg = []
    for res in rank_result:
        msg.append(f"{await get_card_with_cache(res['_id'])}：{res['count']}")
    await rank.finish("\n".join(msg))


@random_idiom_poke.handle()
@random_idiom_command.handle()
async def _(bot: Bot, event: Event):
    if get_ratelimited("daily_idiom"):
        return
    set_ratelimited("daily_idiom", 2)
    random_idiom = await get_random_idiom()
    img_filename = f"{random_idiom['image_hash']}.{random_idiom['image_ext']}"
    img = await ei_img_storage_download(img_filename)
    await bot.send(event, MessageSegment.image(img))


@add_tags.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await add_tags.finish(tips_no_permission)
    if len(args) == 0:
        await add_tags.finish("请输入要添加标签的ID。")
    args = str(args).split()
    image_hash = args[0]
    tags = args[1:]
    image_hash = await hash_extender(image_hash, event.group_id)
    print(image_hash, tags)
    await add_tags_by_hash(image_hash, tags)
    await es_add_tags_by_hash(image_hash, tags)
    await add_tags.finish("已添加标签。")


@calculate_hash.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    args = str(args)
    calculate_hash.finish(await hash_extender(args, event.group_id))


@approve_idiom.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await approve_idiom.finish(tips_no_permission)
    if len(args) == 0:
        await approve_idiom.finish("请输入要审核的ID。")
    image_hashes = str(args).split()

    for image_hash in image_hashes:
        image_hash = await hash_extender(image_hash, event.group_id)
        image_ext = await get_ext_by_image_hash(image_hash)
        image_bytes = await ei_img_storage_download(f"{image_hash}.{image_ext}")
        ocr_text = await get_ocr_text_cloud(image_bytes)
        await update_ocr_text_by_image_hash(image_hash, ocr_text)
        await es_update_ocr_text(image_hash, ocr_text)
        await update_review_status_by_image_hash(image_hash, False)

    upload_ok_quote = await check_dedup(image_hashes, "")

    await approve_idiom.finish("已审核。"+upload_ok_quote)


@reject_idiom.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await reject_idiom.finish(tips_no_permission)
    if len(args) == 0:
        await reject_idiom.finish("请输入要审核的ID。")
    image_hashes = str(args).split()
    for image_hash in image_hashes:
        image_hash = await hash_extender(image_hash, event.group_id)
        image_current_reviewing_status = await get_review_status_by_image_hash(image_hash)
        if image_current_reviewing_status == True:
            logger.info(
                f"Rejected idiom {image_hash} is already rejected, so delete it instead.")
            image_ext = await get_ext_by_image_hash(image_hash)
            await delete_idiom_by_image_hash(image_hash)
            await es_delete_idiom_by_image_hash(image_hash)
            await ei_img_storage_delete(f"{image_hash}.{image_ext}")

        else:
            logger.info(
                f"Rejected idiom {image_hash} is not rejected, so reject it.")
            await update_review_status_by_image_hash(image_hash, True)

        await reject_idiom.finish("已审核。")


@review_list.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await review_list.finish(tips_no_permission)
    idiom_list = await get_under_review_idioms()
    result = ""
    for idiom in idiom_list:
        result += f"hash: {idiom['image_hash']} tags:{idiom['tags']} cat:{idiom['catalogue']} com:{idiom['comment']}"
        result += MessageSegment.image(await ei_img_storage_download(f"{idiom['image_hash']}.{idiom['image_ext']}"))
        result += "\n-----------\n"
    if result == "":
        result = "没有待审核的怡闻录。"
    else:
        result[-1] = str(result[-1])[:-len("\n-----------\n")]
        result[-1] = MessageSegment.text(result[-1])
        result = await message_striper(result)
    await review_list.finish(result)


@pull_image.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await pull_image.finish(tips_no_permission)
    if len(args) == 0:
        await pull_image.finish("请输入要调取的hash。")
    image_hash = str(args)
    image_hash = await hash_extender(image_hash, event.group_id)
    image_ext = await get_ext_by_image_hash(image_hash)
    image = await ei_img_storage_download(f"{image_hash}.{image_ext}")
    await pull_image.finish(MessageSegment.image(image))


@ei_help.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    ei_help_msg = """0. 首先at怡闻录bot

1. 投稿：投稿 标签1 标签2 cat=分类1,分类2 com=备注1,备注2 [图片1] [图片2]

2. 查询：查询 标签1 标签2 cat=分类1,分类2 com=备注1,备注2 [图片1] [图片2]

"""
    ei_admin_help_msg = """管理员命令：
1. 审核通过：通过 ID
2. 审核不通过：打回 ID
3. 调取图片：调取 ID
4. 审核列表：待审核列表(只取10条)
"""
    ei_non_admin_help_msg = "剩下的都是管理员命令，不告诉你。"
    ep_alias_text = ""
    for k, v in ep_alias.items():
        ep_alias_text += f"{v[0]}: {','.join(v)}\n"
    ep_alias_text = ep_alias_text.strip()
    if event.get_user_id() in ei_upload_whitelist:
        ei_help_msg += ei_admin_help_msg
    else:
        ei_help_msg += ei_non_admin_help_msg
    ei_help_msg += "\n----------------\n"
    ei_help_msg += f"[注1]可用分类：\n{ep_alias_text}"
    await ei_help.finish(ei_help_msg)


@test_ap.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    parsed_args = await ei_argparser(args)
    result = list()
    for k, v in parsed_args.items():
        result.append(f"{k}: {v}")
    await test_ap.finish("\n".join(result))


@edit.handle()
async def _(bot: Bot, event: Event, args: Message = CommandArg()):
    if event.get_user_id() not in ei_upload_whitelist:
        await edit.finish(tips_no_permission)
    if len(args) == 0:
        await edit.finish("请输入要编辑的ID。")
    args = str(args).split()
    image_hash = str(args[0])
    image_hash = await hash_extender(image_hash, event.group_id)
    rest_args = args[1:]
    parsed_args = await ei_argparser(rest_args)
    if parsed_args["tag"] is not None:
        await edit_tags_by_hash(image_hash, parsed_args["tag"])
    if parsed_args["cat"] is not None:
        await edit_catalogue_by_image_hash(image_hash, parsed_args["cat"])
    if parsed_args["com"] is not None:
        await edit_comment_by_image_hash(image_hash, parsed_args["com"])
    await edit.finish("编辑完成。")
