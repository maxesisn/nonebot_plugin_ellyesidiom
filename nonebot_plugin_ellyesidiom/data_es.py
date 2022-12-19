from elasticsearch import Elasticsearch
from numpy import indices
from sympy import true

from .consts import global_config

es_scheme = "http"
es_host: str = global_config.es_host
es_port: int = global_config.es_port
es_index: str = global_config.es_index
es_user = global_config.es_user
es_pass = global_config.es_pass


es = Elasticsearch(f"{es_scheme}://{es_host}:{es_port}",
                   http_auth=(es_user, es_pass))


async def search_idiom(query_str: str) -> dict:
    return es.search(index=es_index, query={
        "bool": {
            "must": {
                "multi_match": {
                    "query": query_str,
                    "fields": ["tags^10", "ocr_text"]
                }
            }
        }
    }
    )


async def add_idiom(tags: list[str], image_hash: str, ocr_text: list[str], under_review: bool) -> dict:
    body = {
        "image_hash": image_hash,
        "tags": tags,
        "ocr_text": ocr_text,
        "under_review": under_review
    }
    return es.index(index=es_index, document=body, refresh=True)


async def delete_idiom_by_image_hash(hash: str) -> dict:
    return es.delete_by_query(index=es_index, query={
        "term": {
            "image_hash": hash
        }
    }, refresh=True)


async def update_ocr_text(image_hash: str, ocr_text: list[str]) -> dict:
    return es.update_by_query(index=es_index, query={
        "match": {
            "image_hash": image_hash
        }
    },

        script={
            "source": "ctx._source.ocr_text = params.ocr_text",
            "lang": "painless",
            "params": {
                "ocr_text": ocr_text
            }
    },
        refresh=True
    )


async def add_tags_by_hash(id: str, tags: list[str]) -> dict:
    return es.update_by_query(index=es_index, query={
        "term": {
            "image_hash": id
        }},
        script={
            "source": "ctx._source.tags.addAll(params.tags)",
            "lang": "painless",
            "params": {
                "tags": tags
            }
    },
        refresh=True
    )


async def find_similar_idioms_by_ocr_text(ocr_text: str) -> dict:
    return es.search(index=es_index, query={
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": ocr_text,
                        "fields": ["ocr_text"]
                    }
                },
                {
                    "term": {
                        "under_review": False
                    }
                }
            ]
        }
    }
    )


async def update_under_review_by_hash(id: str, under_review: bool) -> dict:
    return es.update_by_query(index=es_index, query={
        "term": {
            "image_hash": id
        }
    },
        script={
            "source": "ctx._source.under_review = params.under_review",
            "lang": "painless",
            "params": {
                "under_review": under_review
            }
    }, refresh=True)
