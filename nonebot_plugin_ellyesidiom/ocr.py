import base64
import re
import json
from cnocr import CnOcr
from tencentcloud.common import credential
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.ocr.v20181119 import ocr_client, models
from io import BytesIO
from PIL import Image
from nonebot.log import logger

from .consts import global_config

qcloud_api_sid = global_config.qcloud_api_sid
qcloud_api_skey = global_config.qcloud_api_skey
cloud_ocr_method = global_config.cloud_ocr_method

qcloud_cred = credential.Credential(qcloud_api_sid, qcloud_api_skey)
qcloud_ocr_client = ocr_client.OcrClient(qcloud_cred, "ap-beijing")

ocr = CnOcr(det_model_name="db_resnet34", rec_model_name="densenet_lite_136-gru", det_model_backend="pytorch", rec_model_backend="pytorch")

async def clean_ocr_text(ocr_text: list[dict]) -> list[dict]:
    text_blacklist_partial = ["问怡宝一律", "问怡宝回答是", "问怡宝绿帽", "Hoshino", "星乃花园#", "人在线", "相亲相爱", "怡讯大厦", "番灵装", "星乃4.5群之", "怡甸园"]
    text_blacklist_fullmatch = ["发送", "取消", "<返回"]
    text_blacklist_regex = [r"^(上午|下午)?([0-1]?[0-9]|2[0-3]):[0-5][0-9]$", "Hoshino(.*)花园", "^LV(.*)?(群主|管理员)$"]
    cleaned_ocr_text = list()
    for i in ocr_text:
        if any(blacklisted_text in i["text"] for blacklisted_text in text_blacklist_partial):
            print("Partial match:", i["text"])
            continue
        if i["text"] in text_blacklist_fullmatch:
            print("Full match:", i["text"])
            continue
        if len(i["text"]) == 1:
            print("Single character:", i["text"])
            continue
        if any(re.match(regex, i["text"], re.IGNORECASE) for regex in text_blacklist_regex):
            print("Regex match:", i["text"])
            continue
        cleaned_ocr_text.append(i)
    return cleaned_ocr_text

async def calculate_height_from_ndarray(array):
    top_left = array[0]
    top_right = array[1]
    bottom_right = array[2]
    bottom_left = array[3]

    left_height = abs(top_left[1] - bottom_left[1])
    right_height = abs(top_right[1] - bottom_right[1])
    height = (left_height + right_height) / 2
    return height

async def ocr_text_analyze(ocr_result: list[dict]) -> list[dict]:
    text_height_list = list()
    for i in ocr_result:
        text_height_list.append(i["height"])
    text_height_list.sort()
    text_height_gap_list = list()
    for i in range(len(text_height_list) - 1):
        text_height_gap_list.append(text_height_list[i + 1] - text_height_list[i])
    text_height_gap_list.sort()
    print(text_height_list)
    if len(text_height_list) == 0:
        return None
    text_height_average = sum(text_height_list) / len(text_height_list)
    text_height_average = text_height_average
    print("Average height:", text_height_average)
    text_list = list()
    # if all values in text_height_gap_list are the lower than 3
    if not all(i < 3 for i in text_height_gap_list):
        for i in ocr_result:
            if i["height"] >= text_height_average:
                text_list.append(i)
    else:
        print("PCQQ text mode scenario detected.")
        text_list = ocr_result
    return text_list


async def get_ocr_text_local(image) -> list[str]:
    image = Image.open(BytesIO(image))
    ocr_temp_result = ocr.ocr(image)
    ocr_result = list()
    for ocr_temp_result_element in ocr_temp_result:
        temp_dict = dict()
        temp_dict["text"] = ocr_temp_result_element["text"]
        temp_dict["height"] = await calculate_height_from_ndarray(ocr_temp_result_element["position"])
        ocr_result.append(temp_dict)
    ocr_result = await clean_ocr_text(ocr_result)
    print(ocr_result)
    filtered_ocr_text = await ocr_text_analyze(ocr_result)
    if filtered_ocr_text:
        print(filtered_ocr_text)
        filtered_ocr_text = [i["text"] for i in filtered_ocr_text]
        return filtered_ocr_text
    else:
        return None

async def get_ocr_text_qcloud_basic(image) -> list[str]:
    image = base64.b64encode(image).decode()
    req = models.GeneralBasicOCRRequest()
    req.ImageBase64 = image
    try:
        res = qcloud_ocr_client.GeneralBasicOCR(request=req)
    except Exception as e:
        print(e)
        return []
    res_json = res.to_json_string()
    res_dict = json.loads(res_json)
    ocr_result = list()
    for i in res_dict["TextDetections"]:
        if i["DetectedText"] == " ":
            continue
        temp_dict = dict()
        temp_dict["text"] = i["DetectedText"]
        temp_dict["height"] = i["ItemPolygon"]["Height"]
        ocr_result.append(temp_dict)
    print("raw:", ocr_result)
    ocr_result =  await ocr_text_analyze(ocr_result)
    if ocr_result:
        print("analyzed:", ocr_result)
        filtered_ocr_text = await clean_ocr_text(ocr_result)
        print("filtered:", filtered_ocr_text)
        filtered_ocr_text = [i["text"] for i in filtered_ocr_text]
        return filtered_ocr_text
    else:
        return None

async def get_ocr_text_qcloud_accurate(image) -> list[str]:
    image = base64.b64encode(image).decode()
    req = models.GeneralAccurateOCRRequest()
    req.ImageBase64 = image
    try:
        res = qcloud_ocr_client.GeneralAccurateOCR(request=req)
    except Exception as e:
        print(e)
        return []
    res_json = res.to_json_string()
    res_dict = json.loads(res_json)
    ocr_result = list()
    for i in res_dict["TextDetections"]:
        if i["DetectedText"] == " ":
            continue
        temp_dict = dict()
        temp_dict["text"] = i["DetectedText"]
        temp_dict["height"] = i["ItemPolygon"]["Height"]
        ocr_result.append(temp_dict)
    print("raw:", ocr_result)
    ocr_result =  await ocr_text_analyze(ocr_result)
    if ocr_result:
        print("analyzed:", ocr_result)
        filtered_ocr_text = await clean_ocr_text(ocr_result)
        print("filtered:", filtered_ocr_text)
        filtered_ocr_text = [i["text"] for i in filtered_ocr_text]
        return filtered_ocr_text
    else:
        return None

async def get_ocr_text_cloud(image: bytes) -> list[str]:
    match cloud_ocr_method:
        case "qcloud_accurate":
            logger.debug("Using QCloud accurate OCR.")
            return await get_ocr_text_qcloud_accurate(image)
        case "qcloud_basic":
            logger.debug("Using QCloud basic OCR.")
            return await get_ocr_text_qcloud_basic(image)
        case _:
            logger.debug("Using default (QCloud basic OCR).")
            return await get_ocr_text_qcloud_basic(image)
