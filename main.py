from sseclient import SSEClient

def sse_stream_iterator(url):
    """
    Connect to an HTTP SSE stream server and iterate over received messages.

    :param url: The URL of the desired SSE stream source.
    """
    request_headers = {'User-Agent': 'etl-wikipedia-updates'} # Include an identifier in the request header to prevent a 403
    messages = SSEClient(url, headers=request_headers)

    for message in messages:
        print(message)

def main():
    sse_stream_iterator('https://stream.wikimedia.org/v2/stream/recentchange')

if __name__ == "__main__":
    main()
