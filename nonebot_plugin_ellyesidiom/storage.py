import os

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos import CosServiceError
from qcloud_cos import CosClientError

from .config import global_config

ei_img_storage_bucket = global_config.ei_img_storage_bucket
ei_img_storage_region = global_config.ei_img_storage_region
ei_img_storage_id: str = global_config.ei_img_storage_id
ei_img_storage_key: str = global_config.ei_img_storage_key
cache_dir = global_config.cache_dir

cos_config = CosConfig(Region=ei_img_storage_region, SecretId=ei_img_storage_id, SecretKey=ei_img_storage_key)
cos_client = CosS3Client(cos_config)

async def ei_img_storage_upload(filename:str, filebytes:bytes):
    try:
        response = cos_client.put_object(
            Bucket=ei_img_storage_bucket,
            Body=filebytes,
            Key=filename,
            EnableMD5=False
        )
    except CosServiceError as e:
        print(e.get_error_code())
        print(e.get_error_msg())
        print(e.get_resource_location())
        return False
    except CosClientError as e:
        print(e.get_error_code())
        print(e.get_error_msg())
        return False
    return response

async def ei_img_storage_download(filename:str):
    # search filename in cache folder
    filelist = os.listdir(cache_dir)
    if filename in filelist:
        image_bytes = open(os.path.join(cache_dir, filename), "rb")
        return image_bytes.read()
    else:
        image_bytes = await ei_img_storage_download_fallback(filename)
        with open(os.path.join(cache_dir, filename), "wb") as f:
            f.write(image_bytes)
        return image_bytes


async def ei_img_storage_download_fallback(filename:str):
    try:
        response = cos_client.get_object(
            Bucket=ei_img_storage_bucket,
            Key=filename
        )
    except CosServiceError as e:
        print(e.get_error_code())
        print(e.get_error_msg())
        print(e.get_resource_location())
        return False
    except CosClientError as e:
        print(e.get_error_code())
        print(e.get_error_msg())
        return False
    image_bytes = response["Body"]
    image_bytes = image_bytes.get_raw_stream().read()
    return image_bytes

async def ei_img_storage_delete(filename:str):
    try:
        response = cos_client.delete_object(
            Bucket=ei_img_storage_bucket,
            Key=filename
        )
    except CosServiceError as e:
        print(e.get_error_code())
        print(e.get_error_msg())
        print(e.get_resource_location())
        return False
    except CosClientError as e:
        print(e.get_error_code())
        print(e.get_error_msg())
        return False
    return response