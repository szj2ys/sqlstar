# *_*coding:utf-8 *_*
from os.path import dirname, abspath, join

ROOT = dirname(abspath(__file__))
import click
from click_help_colors import HelpColorsGroup

# CONSTANTS
CLICK_CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
CLICK_CONTEXT_SETTINGS_NO_HELP = dict(help_option_names=[])


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    package_conf = {}
    with open(join(ROOT, "__version__.py")) as f:
        exec(f.read(), package_conf)
    click.secho(package_conf['__version__'], blink=True, bold=True)
    ctx.exit()


@click.group(cls=HelpColorsGroup,
             context_settings=CLICK_CONTEXT_SETTINGS,
             help_headers_color='yellow',
             help_options_color='magenta',
             help_options_custom_colors={
                 'version': 'green',
             })
@click.option('-v',
              '--version',
              is_flag=True,
              callback=print_version,
              expose_value=False,
              is_eager=True,
              help='Show version')
def cli():
    """\b
███████╗ ██████╗ ██╗     ███████╗████████╗ █████╗ ██████╗
██╔════╝██╔═══██╗██║     ██╔════╝╚══██╔══╝██╔══██╗██╔══██╗
███████╗██║   ██║██║     ███████╗   ██║   ███████║██████╔╝
╚════██║██║▄▄ ██║██║     ╚════██║   ██║   ██╔══██║██╔══██╗
███████║╚██████╔╝███████╗███████║   ██║   ██║  ██║██║  ██║
╚══════╝ ╚══▀▀═╝ ╚══════╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝
    """


def run():
    try:
        cli()
    except Exception as error:
        pass


if __name__ == "__main__":
    run()
