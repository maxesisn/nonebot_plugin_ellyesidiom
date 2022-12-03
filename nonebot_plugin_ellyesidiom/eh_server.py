from .data_mongo import get_latest_25, get_idiom_by_image_hash
from .data_mongo import get_under_review_idioms
from .cat_checker import id_to_ep_alias
from .data_es import search_idiom
from .consts import global_config

from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from nonebot import get_app
from nonebot.log import logger

ei_img_storage_bucket = global_config.ei_img_storage_bucket
ei_img_storage_region = global_config.ei_img_storage_region

app: FastAPI = get_app()

router = APIRouter()

@router.get("/api/index")
async def index():
    mg_data = await get_latest_25()
    payload = []
    for data in mg_data:
        temp_dict = {}
        if "tags" in data and data["tags"]:
            temp_dict["title"] = " ".join(data["tags"])
        else:
            temp_dict["title"] = ""
        subtitle_str = ""
        cat_name = list()
        for cat in data["catalogue"]:
            cat_name.append(await id_to_ep_alias(cat))
        cat_name = " ".join(cat_name)
        com_str = " ".join(data["comment"]) or "无"
        subtitle_str = f"备注:{com_str} 分类:{cat_name}"
        temp_dict["subtitle"] = subtitle_str
        image_url = data["image_hash"] + "." + data["image_ext"]
        image_url = f"https://{ei_img_storage_bucket}.cos.{ei_img_storage_region}.myqcloud.com/" + image_url
        temp_dict["img"] = image_url
        payload.append(temp_dict)
    return JSONResponse(payload)


@router.get("/api/search")
async def search(keyword: str):
    search_res = await search_idiom(keyword)
    search_count = search_res["hits"]["total"]["value"]
    if search_count == 0:
        return JSONResponse({"status": "no result"})
    search_res = search_res["hits"]["hits"]
    payload = []
    for data in search_res:
        data_hash = data["_source"]["image_hash"]
        data = await get_idiom_by_image_hash(data_hash)
        print(data)
        temp_dict = {}
        if "tags" in data and data["tags"]:
            temp_dict["title"] = " ".join(data["tags"])
        else:
            temp_dict["title"] = ""
        subtitle_str = ""
        cat_name = list()
        print(data, type(data))
        if "catalogue" in data and data["catalogue"]:
            for cat in data["catalogue"]:
                cat_name.append(await id_to_ep_alias(cat))
            cat_name = " ".join(cat_name)
        else:
            cat_name = "怡宝"
        if "comment" in data and data["comment"]:
            com_str = " ".join(data["comment"])
        else:
            com_str = "无"
        subtitle_str = f"备注:{com_str} 分类:{cat_name}"
        temp_dict["subtitle"] = subtitle_str
        image_url = data["image_hash"] + "." + data["image_ext"]
        image_url = f"https://{ei_img_storage_bucket}.cos.{ei_img_storage_region}.myqcloud.com/" + image_url
        temp_dict["img"] = image_url
        payload.append(temp_dict)
    print(payload)
    return JSONResponse(payload)

@router.post("/api/admin_auth")
async def admin_auth():
    return JSONResponse({"status": "ok"})

@router.get("/api/is_admin")
async def is_admin():
    return JSONResponse({"status": "ok"})

@router.get("/api/get_review_list")
async def get_review_list():
    data = await get_under_review_idioms()
    payload = []
    for d in data:
        temp_dict = {}
        temp_dict["img"] = f"https://{ei_img_storage_bucket}.cos.{ei_img_storage_region}.myqcloud.com/" + d["image_hash"] + "." + d["image_ext"]
        temp_dict["tags"] = d["tags"]
        temp_dict["comment"] = d["comment"]
        temp_dict["catalogue"] = d["catalogue"]
        payload.append(temp_dict)
    return JSONResponse(payload)

    

app.include_router(router)
logger.info("EllyeHub API Server Started")