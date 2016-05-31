import logging.config

from hdx.utilities.loader import load_and_merge_data


def setup_logging(
        input_type: str = 'yaml',
        logging_cfg='config/logging.yml',
        smtp_logging_cfg='config/smtp_logging.yml'
):
    """Setup logging configuration

    """
    config = load_and_merge_data(input_type, [logging_cfg, smtp_logging_cfg])
    logging.config.dictConfig(config)
