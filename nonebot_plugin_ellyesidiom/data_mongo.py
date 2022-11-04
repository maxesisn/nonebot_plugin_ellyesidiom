import datetime
from pymongo import MongoClient

from .utils import shanghai_tz, global_config

from bson.codec_options import CodecOptions

codec_opt = CodecOptions(tz_aware=True, tzinfo=shanghai_tz)


mongo_host = global_config.mongo_host
mongo_user = global_config.mongo_user
mongo_pass = global_config.mongo_pass

client = MongoClient(f'mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/')

ei_data = client.ei_data

idioms_data = ei_data.idioms.with_options(codec_options=codec_opt)

async def get_idiom_by_image_hash(image_hash: str) -> dict:
    return idioms_data.find_one({"image_hash": image_hash})

async def add_idiom(tags: list[str], image_hash: str, image_ext:str, ocr_text: list[str], uploader_info: dict, under_review: bool, comment: list[str], catalogue: list[str]) -> dict:
    body = {
        "tags": tags,
        "image_hash": image_hash,
        "image_ext": image_ext,
        "ocr_text": ocr_text,
        "uploader": uploader_info,
        "under_review": under_review,
        "comment": comment,
        "catalogue": catalogue,
        "timestamp": datetime.datetime.now(shanghai_tz)
    }
    print(comment, "comment")
    return idioms_data.insert_one(body)

async def delete_idiom_by_image_hash(image_hash: str) -> None:
    idioms_data.delete_one({"image_hash": image_hash})

async def update_ocr_text_by_image_hash(image_hash: str, ocr_text: list[str]) -> None:
    idioms_data.update_one({"image_hash": image_hash}, {"$set": {"ocr_text": ocr_text}})

async def count_under_review() -> int:
    return idioms_data.count_documents({"under_review": True})

async def count_reviewed() -> int:
    return idioms_data.count_documents({"under_review": False})

async def add_tags_by_hash(image_hash: str, tags: list[str]) -> None:
    idioms_data.update_one({"image_hash": image_hash}, {"$addToSet": {"tags": {"$each": tags}}})

async def edit_tags_by_hash(image_hash: str, tags: list[str]) -> None:
    idioms_data.update_one({"image_hash": image_hash}, {"$set": {"tags": tags}})

async def edit_comment_by_image_hash(image_hash: str, comment: list[str]) -> None:
    idioms_data.update_one({"image_hash": image_hash}, {"$set": {"comment": comment}})

async def edit_catalogue_by_image_hash(image_hash: str, catalogue: list[str]) -> None:
    idioms_data.update_one({"image_hash": image_hash}, {"$set": {"catalogue": catalogue}})

async def get_id_by_image_hash(image_hash: str) -> str:
    return idioms_data.find_one({"image_hash": image_hash})["_id"]

async def get_ext_by_image_hash(image_hash: str) -> str:
    return idioms_data.find_one({"image_hash": image_hash})["image_ext"]

async def check_image_hash_exists(image_hash: str) -> bool:
    return idioms_data.count_documents({"image_hash": image_hash}) > 0

async def update_review_status_by_image_hash(image_hash: str, under_review: bool) -> None:
    idioms_data.update_one({"image_hash": image_hash}, {"$set": {"under_review": under_review}})

async def get_review_status_by_image_hash(image_hash: str) -> bool:
    return idioms_data.find_one({"image_hash": image_hash})["under_review"]

async def get_under_review_idioms() -> list[dict]:
    return idioms_data.find({"under_review": True})

async def check_ocr_text_exists(ocr_text: list[str]) -> bool:
    return idioms_data.count_documents({"ocr_text": ocr_text}) > 0

async def get_idiom_by_catalogue(catalogue: str) -> list[dict]:
    return idioms_data.find({"catalogue": catalogue})

async def get_idiom_by_comment(comment: str) -> list[dict]:
    return idioms_data.find({"comment": comment})

async def get_catalogue_by_image_hash(image_hash: str) -> list[str]:
    return idioms_data.find_one({"image_hash": image_hash})["catalogue"]

async def get_comment_by_image_hash(image_hash: str) -> list[str]:
    return idioms_data.find_one({"image_hash": image_hash})["comment"]