"""LAN admission and the transport boundary for remote worker credentials."""
import ipaddress
import socket


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


def trusted_networks(values):
    if values is None:
        return LAN_NETWORKS
    if not isinstance(values, (list, tuple)):
        raise ValueError('trusted_networks must be a list of LAN CIDR networks')
    return tuple(ipaddress.ip_network(value, strict=False) for value in values)
