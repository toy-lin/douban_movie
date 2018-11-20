import logging

logger = None


def get_logger(config):
    global logger
    if logger:
        return logger

    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    fh = logging.FileHandler(config['logger']['log_file'], mode='a')
    fh.setLevel(logging.NOTSET)

    formatter = logging.Formatter("%(asctime)s -%(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
