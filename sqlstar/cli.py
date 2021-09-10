import os
from pathlib import Path

import click
from click_help_colors import HelpColorsGroup


@click.group(cls=HelpColorsGroup,
             help_headers_color='yellow',
             help_options_color='magenta',
             help_options_custom_colors={
                 'version': 'green',
             })
def cli():
    """\b

    """


# http://patorjk.com/software/taag/#p=display&h=0&v=0&f=Graffiti&t=funlp
@cli.command(help='Print version.')
def version():
    here = Path(__file__).parent.absolute()
    package_conf = {}
    with open(os.path.join(here, "__version__.py")) as f:
        exec(f.read(), package_conf)
    print(package_conf['__version__'])


def run():
    try:
        cli()
    except Exception as e:
        pass


if __name__ == "__main__":
    run()
