"""LAN admission and the transport boundary for remote worker credentials."""
import ipaddress
import socket
from urllib.parse import urlsplit, urlunsplit


LAN_NETWORKS = tuple(ipaddress.ip_network(value) for value in (
    '127.0.0.0/8', '::1/128', '10.0.0.0/8', '172.16.0.0/12',
    '192.168.0.0/16', '169.254.0.0/16', 'fc00::/7', 'fe80::/10'))


def in_networks(address, networks=LAN_NETWORKS):
    try:
        value = ipaddress.ip_address(str(address).split('%', 1)[0])
        value = getattr(value, 'ipv4_mapped', None) or value
        return any(value.version == network.version and value in network for network in networks)
    except ValueError:
        return False


def lan_endpoint(host):
    try:
        ipaddress.ip_address(host)
    except ValueError:
        try:
            addresses = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except OSError:
            return False
        return bool(addresses) and all(in_networks(item[4][0]) for item in addresses)
    return in_networks(host)


def node_url(address):
    """Expand an operator's host shorthand without downgrading public TLS."""
    address = address.strip()
    explicit = '://' in address
    if not explicit:
        try:
            if ipaddress.ip_address(address).version == 6:
                address = '[' + address + ']'
        except ValueError:
            pass
    parsed = urlsplit(address if explicit else '//' + address)
    if (not parsed.hostname or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment or parsed.path not in ('', '/')
            or explicit and parsed.scheme not in ('http', 'https', 'local')):
        raise ValueError('Node address: host[:port] or http(s)://host[:port]')
    if parsed.scheme == 'local':
        return address.rstrip('/')
    port = parsed.port if parsed.port is not None else 26133
    if not 0 < port < 65536:
        raise ValueError('Node port must be between 1 and 65535')
    scheme = parsed.scheme if explicit else ('http' if lan_endpoint(parsed.hostname) else 'https')
    host = '[' + parsed.hostname + ']' if ':' in parsed.hostname else parsed.hostname
    return urlunsplit((scheme, host + ':' + str(port), '', '', ''))


def trusted_networks(values):
    if values is None:
        return LAN_NETWORKS
    if not isinstance(values, (list, tuple)):
        raise ValueError('trusted_networks must be a list of LAN CIDR networks')
    return tuple(ipaddress.ip_network(value, strict=False) for value in values)
