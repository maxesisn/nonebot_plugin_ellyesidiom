import datetime
from pymongo import MongoClient

from .consts import shanghai_tz, global_config

from bson.codec_options import CodecOptions

codec_opt = CodecOptions(tz_aware=True, tzinfo=shanghai_tz)


mongo_host = global_config.mongo_host
mongo_user = global_config.mongo_user
mongo_pass = global_config.mongo_pass

client = MongoClient(f'mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/')

ei_data = client.ei_data
me_data = client.me_data

idioms_data = ei_data.idioms.with_options(codec_options=codec_opt)
greylist_data = ei_data.greylist.with_options(codec_options=codec_opt)
cards_data = me_data.cards.with_options(codec_options=codec_opt)

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
    return idioms_data.find({"under_review": True}).limit(10)

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

async def get_latest_25() -> list[dict]:
    return idioms_data.find().sort("timestamp", -1).limit(25)


async def get_full_hash_by_prefix(prefix: str) -> list[str] | None:
    hash_counts = idioms_data.count_documents({"image_hash": {"$regex": f"^{prefix}"}})
    if hash_counts == 0:
        return None
    elif hash_counts == 1:
        return [idioms_data.find_one({"image_hash": {"$regex": f"^{prefix}"}})["image_hash"]]
    else:
        result = []
        all_hashes = idioms_data.find({"image_hash": {"$regex": f"^{prefix}"}})
        for hash in all_hashes:
            result.append(hash["image_hash"])
        return result

async def get_uploader_by_hash(image_hash: str) -> dict:
    return idioms_data.find_one({"image_hash": image_hash})["uploader"]

# get uploader nickname rank and exclude under_review idioms and platform is not qq
async def get_uploader_rank() -> list[dict]:
    return idioms_data.aggregate([
        {"$match": {"under_review": False, "uploader.platform": {"$eq": "qq"}}},
        {"$group": {"_id": "$uploader.id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])


async def get_gm_info(user_id):
    user_id = str(user_id)
    result: dict = cards_data.find_one({'id': user_id})
    card: str = result['card'] if result else None
    return card

async def set_gm_info(user_id, gm_info):
    user_id = str(user_id)
    result = cards_data.update_one(
        {'id': user_id},
        {'$set': {'card': gm_info}},
        upsert=True
    )
    return result.modified_count

async def get_random_idiom() -> dict:
    return idioms_data.aggregate([{"$sample": {"size": 1}}]).next()

async def get_ocr_text_by_image_hash(image_hash: str) -> list[str]:
    return idioms_data.find_one({"image_hash": image_hash})["ocr_text"]

async def greylist_incr(user_id:str, platform:str) -> int:
    return greylist_data.update_one(
        {"user_id": user_id, "platform": platform},
        {"$inc": {"count": 1}},
        upsert=True
    ).modified_count