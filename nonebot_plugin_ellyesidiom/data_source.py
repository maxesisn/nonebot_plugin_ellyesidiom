from elasticsearch import Elasticsearch
from numpy import indices

from .config import global_config

es_scheme = "http"
es_host: str = global_config.es_host
es_port: int = global_config.es_port
es_index: str = global_config.es_index
es_user = global_config.es_user
es_pass = global_config.es_pass


es = Elasticsearch(f"{es_scheme}://{es_host}:{es_port}",
                   http_auth=(es_user, es_pass))


async def search_idiom(query_str: str) -> dict:
    return es.search(index=es_index, body={
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": query_str,
                        "fields": ["tags^2", "ocr_text"]
                    }
                },
                "must_not": {
                    "term": {
                        "under_review": True
                    }
                }
            }
        }
    }
    )


async def add_idiom(tags: list[str], filename: str, ocr_text: list[str], uploader_info: dict, under_review: bool) -> dict:
    body = {
        "tags": tags,
        "filename": filename,
        "ocr_text": ocr_text,
        "uploader": uploader_info,
        "under_review": under_review
    }
    return es.index(index=es_index, body=body)


async def delete_idiom_by_id(id: str) -> dict:
    return es.delete(index=es_index, id=id)

async def create_index():
    if es.indices.exists(index=es_index):
        es.indices.delete(index=es_index)
    es.indices.create(index=es_index, body={
        "mappings": {
            "properties": {
                "tags": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart"
                },
                "filename": {
                    "type": "keyword"
                },
                "ocr_text": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart"
                },
                "uploader": {
                    "properties": {
                        "user_id": {
                            "type": "keyword",
                        },
                        "nickname": {
                            "type": "keyword"
                        },
                        "group_id": {
                            "type": "keyword"
                        },
                        "group_name": {
                            "type": "keyword"
                        }
                    }
                },
                "under_review": {
                    "type": "boolean"
                }
            }
        }
    })