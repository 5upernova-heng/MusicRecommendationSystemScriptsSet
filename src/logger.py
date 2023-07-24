import os
import logging
import datetime
from rich.highlighter import NullHighlighter
from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger('dev')

logger.setLevel(logging.INFO)

console_hdlr = RichHandler(
    show_path=False,
    show_time=True,
    rich_tracebacks=True,
    tracebacks_show_locals=True,
    tracebacks_extra_lines=3,
)

def set_file_logger():
    log_file = f'../log/{datetime.date.today()}.txt'
    try:
        file = open(log_file, mode='a', encoding='utf-8')
    except FileNotFoundError:
        os.mkdir('../log')
        file = open(log_file, mode='a', encoding='utf-8')

    file_console = Console(
        file=file,
        no_color=True,
        highlight=False,
        width=119,
    )

    hdlr = RichHandler(
        console=file_console,
        show_path=True,
        show_time=False,
        show_level=True,
        rich_tracebacks=True,
        tracebacks_show_locals=True,
        tracebacks_extra_lines=3,
        highlighter=NullHighlighter(),
    )

    logger.addHandler(hdlr)
    logger.log_file = log_file

logger.addHandler(console_hdlr)
set_file_logger()