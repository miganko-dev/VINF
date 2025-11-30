from urllib.parse import urlparse
def extract_domain(url: str) -> str:
    url = url.replace("https://", "").replace("http://", "")
    return url.replace("www.", "")