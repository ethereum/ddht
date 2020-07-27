import logging

from ddht.cli_parser import parser
from ddht.logging import setup_logging

DDHT_HEADER = "\n".join(
    (
        "",
        r"-. .-.   .-. .-.   .-. .-.",
        r"  \ D \ /   \ H \ /   \\",
        r" / \   \ D / \   \ T / \\",
        r"~   `-~ `-`   `-~ `-`   `-",
        "",
    )
)


logger = logging.getLogger("ddht")


async def main() -> None:
    args = parser.parse_args()

    setup_logging(args.log_level)

    logger.info(DDHT_HEADER)

    await args.func(args)
