import logging

def setup_custom_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # handler and formatter
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s "
    )
    handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(handler)

    logging.getLogger("httpx").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("requests").setLevel(logging.CRITICAL)
    logging.getLogger("chardet").setLevel(logging.CRITICAL)
    logging.getLogger("asyncio").setLevel(logging.CRITICAL)
    
    return logger