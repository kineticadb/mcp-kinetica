
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,  # Important to allow Uvicorn's loggers to propagate
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": "app.log",  # The base log file name
            "maxBytes": 1048576,  # 1MB per file
            "backupCount": 5,  # Keep 5 backup files
        },
        "console_handler": {
            "class": "logging.StreamHandler",
            "level": "WARNING",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "": {  # Root logger
            "level": "INFO",
            "handlers": ["file_handler", "console_handler"],
            "propagate": True,
        },
        "uvicorn": {
            "level": "INFO",
            "handlers": ["file_handler", "console_handler"],
            "propagate": False,  # Prevent double logging if root also handles Uvicorn logs
        },
        "uvicorn.access": {
            "level": "INFO",
            "handlers": ["file_handler", "console_handler"],
            "propagate": False,
        },
        "uvicorn.error": {
            "level": "ERROR",
            "handlers": ["file_handler", "console_handler"],
            "propagate": False,
        },
    },
}
