from logging import getLogger
from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from . import settings
from . import storage

logger = getLogger(__name__)


class StaticPagesRenderer:
    """
        static ファイル群をレンダリングする。
    """
    def __init__(
        self,
        fileStorage: storage.SupportStorage,
        basedir: str,
    ):
        self.jinja2_env = Environment(
            loader=PackageLoader('chalicelib', 'templates'),
            autoescape=select_autoescape(['html']),
        )
        self.fileStorage = fileStorage
        self.basedir = basedir

    def render_all(self):
        templates = self.jinja2_env.list_templates()
        static_templates = [t for t in templates if t.startswith('static/')]
        basepath = self.fileStorage.path_object(self.basedir)

        for template_path in static_templates:
            template = self.jinja2_env.get_template(template_path)
            html = template.render(settings=settings)
            filename = Path(template_path).stem + '.html'
            outputpath = str(basepath / filename)
            logger.info('generating a static file "%s"', outputpath)
            stream = self.fileStorage.get_output_stream(outputpath)
            stream.write(html.encode('UTF-8'))
            self.fileStorage.close_output_stream(stream)
