"""Portable bounded batch runtime; copied verbatim into generated Airflow projects.

No Airflow import, code evaluation, retries, ambient proxy or TLS bypass.
"""
import base64
import copy
import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

MAX_CONTENT = 16 * 1024
MAX_ENVELOPE = 32 * 1024


class MappingError(ValueError):
    """Safe failure without response bodies, URLs or credentials."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        raise MappingError('HTTP redirects are not supported by this mapping.')


def envelope(content, attributes=None):
    if not isinstance(content, bytes) or len(content) > MAX_CONTENT:
        raise MappingError('Content exceeds the 16 KiB batch limit.')
    value = {'content_base64': base64.b64encode(content).decode('ascii'), 'attributes': dict(attributes or {})}
    if any(not isinstance(k, str) or not isinstance(v, str) for k,v in value['attributes'].items()):
        raise MappingError('Attributes must be strings.')
    if len(json.dumps(value).encode()) > MAX_ENVELOPE:
        raise MappingError('Flow envelope exceeds the 32 KiB limit.')
    return value


def content(value):
    if not isinstance(value, dict) or set(value) != {'content_base64', 'attributes'}:
        raise MappingError('Invalid batch envelope.')
    try:
        decoded = base64.b64decode(value['content_base64'], validate=True)
        envelope(decoded, value['attributes'])
        return decoded
    except (ValueError, TypeError, KeyError):
        raise MappingError('Invalid batch envelope.') from None


def http_get(config):
    url = config['url']
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.fragment or parsed.query:
        raise MappingError('A literal HTTPS URL without credentials, query or fragment is required.')
    context = ssl.create_default_context()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect(), urllib.request.HTTPSHandler(context=context))
    request = urllib.request.Request(url, headers={'Accept-Encoding':'identity', 'Accept': config.get('accept', '*/*')}, method='GET')
    deadline = time.monotonic() + 30
    try:
        with opener.open(request, timeout=15) as response:
            if not 200 <= response.status < 300:
                raise MappingError('HTTP response was not successful.')
            if response.headers.get('Content-Encoding', 'identity').lower() not in ('identity', ''):
                raise MappingError('Compressed responses require an explicit mapping.')
            chunks=[]; size=0
            read = getattr(response, 'read1', response.read)
            while True:
                if time.monotonic() > deadline:
                    raise MappingError('HTTP response exceeded the time bound.')
                piece=read(min(4096, MAX_CONTENT + 1 - size))
                if not piece: break
                chunks.append(piece); size += len(piece)
                if size > MAX_CONTENT:
                    raise MappingError('HTTP response exceeds the 16 KiB batch limit.')
            attributes={'invokehttp.status.code':str(response.status), 'invokehttp.request.url':url, 'invokehttp.response.url':url}
            if response.headers.get('Content-Type'):
                attributes['mime.type']=response.headers.get('Content-Type').split(';',1)[0].strip()
            return envelope(b''.join(chunks), attributes)
    except MappingError:
        raise
    except Exception:
        raise MappingError('HTTPS GET failed; inspect endpoint access without logging response content.') from None


def execute(config, incoming=None):
    """Execute one checked, finite processor mapping. Input envelopes are immutable."""
    operation=config.get('operation')
    if operation=='http_get':
        if incoming is not None: raise MappingError('HTTP source must have no input.')
        return http_get(config)
    original=content(incoming)
    attributes=copy.deepcopy(incoming['attributes'])
    if operation=='update_attributes':
        attributes.update(config['attributes'])
        return envelope(original, attributes)
    if operation=='replace_text':
        try: text=original.decode('utf-8', errors='strict')
        except UnicodeError: raise MappingError('ReplaceText requires valid UTF-8.') from None
        strategy=config['strategy']; value=config['replacement']
        if strategy=='Literal Replace': text=text.replace(config['search'], value)
        elif strategy=='Append': text += value
        elif strategy=='Prepend': text=value + text
        elif strategy=='Always Replace': text=value
        else: raise MappingError('Unknown replacement strategy.')
        return envelope(text.encode('utf-8'), attributes)
    raise MappingError('Processor has no executable mapping.')
