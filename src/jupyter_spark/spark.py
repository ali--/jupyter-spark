import re

from bs4 import BeautifulSoup
from notebook.utils import url_path_join
from traitlets.config import LoggingConfigurable
from traitlets.traitlets import Unicode
from urllib.parse import urlparse
# try importing lxml and use it as the BeautifulSoup builder if available
try:
    import lxml  # noqa
except ImportError:
    BEAUTIFULSOUP_BUILDER = 'html.parser'
else:
    BEAUTIFULSOUP_BUILDER = 'lxml'  # pragma: no cover

# a regular expression to match paths against the Spark on EMR proxy paths
PROXY_PATH_RE = re.compile(r'\/proxy\/application_\d+_\d+\/(.*)')

# a tuple of tuples with tag names and their attribute to automatically fix
PROXY_ATTRIBUTES = (
    (('a', 'link'), 'href'),
    (('img', 'script'), 'src'),
)


class Spark(LoggingConfigurable):
    """
    A config object that is able to replace URLs of the Spark frontend
    on the fly.
    """
    url = Unicode(
        'http://localhost',
        help='The server where the backends are running (probably localhost)',
    ).tag(config=True)

    proxy_root = Unicode(
        '/proxy',
        help='The URL path under which the Spark API will be proxied',
    )

    def __init__(self, *args, **kwargs):
        self.base_url = kwargs.pop('base_url')
        super(Spark, self).__init__(*args, **kwargs)
        self.proxy_url = url_path_join(self.base_url, self.proxy_root)

    def backend_url(self, request):
        request_path = request.uri[len(self.proxy_url):]
        if not request_path and 'X-Original-Uri' in request.headers:
            request_path = request.headers['X-Original-Uri'].split(self.proxy_root)[1]

        port = request_path[1:] # ignore the first '/'
        request_path = request_path[len(port)+1:]
        new_url = url_path_join(self.url+':'+port, request_path)
        return new_url

    def replace(self, content,port):
        """
        Replace all the links with our prefixed handler links, e.g.:

        /proxy/application_1467283586194_0015/static/styles.css' or
        /static/styles.css

        with

        /proxy/4040/static/styles.css
        """
        soup = BeautifulSoup(content, BEAUTIFULSOUP_BUILDER)
        for tags, attribute in PROXY_ATTRIBUTES:
            for tag in soup.find_all(tags, **{attribute: True}):
                value = tag[attribute]
                match = PROXY_PATH_RE.match(value)
                if match is not None:
                    value = match.groups()[0]

                # handle case of absolute links
                parsed_url = urlparse(value)
                if parsed_url.netloc:
                    value = parsed_url.path
                    if parsed_url.query:
                        value = value + '?' + parsed_url.query
                    port = str(parsed_url.port)

                tag[attribute] = url_path_join(self.proxy_url+'/'+port, value)
        return str(soup)
