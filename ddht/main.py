import logging
import shutil

from ddht.boot_info import BootInfo
from ddht.cli_parser import parser
from ddht.logging import setup_logging
from ddht.xdg import get_xdg_data_home

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

    setup_logging(args.log_file, args.log_level_file, args.log_level_stderr)

    logger.info(DDHT_HEADER)

    boot_info = BootInfo.from_namespace(args)

    if not boot_info.base_dir.exists():
        if boot_info.is_ephemeral or get_xdg_data_home() in boot_info.base_dir.parents:
            boot_info.base_dir.mkdir(exist_ok=True)
        else:
            raise FileNotFoundError(
                "Not creating DDHT root directory as it is not present and is "
                f"not under the $XDG_DATA_HOME: {boot_info.base_dir}"
            )

    try:
        await args.func(boot_info)
    finally:
        if boot_info.is_ephemeral:
            shutil.rmtree(boot_info.base_dir)
