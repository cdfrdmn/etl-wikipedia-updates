from sseclient import SSEClient
from config import config

USER_AGENT = 'etl-wikipedia-updates/0.1-dev (contact: 43573338+cdfrdmn@users.noreply.github.com)'

def sse_stream_iterator(url):
    """
    Connect to an HTTP SSE stream server and iterate over received messages.

    :param url: The URL of the desired SSE stream source.
    """
    request_headers = {'User-Agent': USER_AGENT} # Include an identifier in the request header to prevent a 403
    messages = SSEClient(url, headers=request_headers)

    for message in messages:
        print(message)

def main():
    sse_stream_iterator(config.WIKI_STREAM_URL)

if __name__ == "__main__":
    main()
