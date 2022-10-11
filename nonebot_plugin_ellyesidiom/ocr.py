from cnocr import CnOcr
from io import BytesIO
from PIL import Image

import logging.config
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True,
})


ocr = CnOcr(rec_model_name="densenet_lite_136-gru") 

async def get_ocr_text(image):
    image = Image.open(BytesIO(image))
    return ocr.ocr(image)