"""One-time native HTTPS configuration for a worker's public IP endpoint."""
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import subprocess
import time

from .bundle import atomic_json
from .client import Client, RemoteError


_WINDOWS_CERTIFICATE = r'''
$ErrorActionPreference = 'Stop'
try {
    $directory = [IO.Path]::GetDirectoryName($env:DCO_TLS_CERT)
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    & "$env:SystemRoot\System32\icacls.exe" $directory /inheritance:r /grant:r "*${sid}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not restrict the TLS key directory.' }
    $rsa = [Security.Cryptography.RSACng]::new(2048)
    try {
        $request = [Security.Cryptography.X509Certificates.CertificateRequest]::new(
            'CN=DCO worker', $rsa, [Security.Cryptography.HashAlgorithmName]::SHA256,
            [Security.Cryptography.RSASignaturePadding]::Pkcs1)
        $san = [Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder]::new()
        $san.AddIpAddress([Net.IPAddress]::Parse($env:DCO_TLS_ADDRESS))
        $san.AddIpAddress([Net.IPAddress]::Loopback)
        $san.AddIpAddress([Net.IPAddress]::IPv6Loopback)
        $san.AddDnsName('localhost')
        $request.CertificateExtensions.Add($san.Build())
        $request.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($false, $false, 0, $true))
        $usage = [Security.Cryptography.X509Certificates.X509KeyUsageFlags]::DigitalSignature -bor [Security.Cryptography.X509Certificates.X509KeyUsageFlags]::KeyEncipherment
        $request.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509KeyUsageExtension]::new($usage, $true))
        $oids = [Security.Cryptography.OidCollection]::new()
        [void]$oids.Add([Security.Cryptography.Oid]::new('1.3.6.1.5.5.7.3.1'))
        $request.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509EnhancedKeyUsageExtension]::new($oids, $false))
        $request.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509SubjectKeyIdentifierExtension]::new($request.PublicKey, $false))
        $certificate = $request.CreateSelfSigned([DateTimeOffset]::UtcNow.AddMinutes(-5), [DateTimeOffset]::UtcNow.AddYears(3))
        try {
            $options = [Base64FormattingOptions]::InsertLineBreaks
            $key = $rsa.Key.Export([Security.Cryptography.CngKeyBlobFormat]::Pkcs8PrivateBlob)
            [IO.File]::WriteAllText($env:DCO_TLS_KEY, "-----BEGIN PRIVATE KEY-----`n" + [Convert]::ToBase64String($key, $options) + "`n-----END PRIVATE KEY-----`n", [Text.Encoding]::ASCII)
            [IO.File]::WriteAllText($env:DCO_TLS_CERT, "-----BEGIN CERTIFICATE-----`n" + [Convert]::ToBase64String($certificate.RawData, $options) + "`n-----END CERTIFICATE-----`n", [Text.Encoding]::ASCII)
        } finally { $certificate.Dispose() }
    } finally { $rsa.Dispose() }
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
'''


def _make_certificate(cert, key, address):
    cert.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == 'nt':
        environment = dict(os.environ, DCO_TLS_CERT=str(cert), DCO_TLS_KEY=str(key), DCO_TLS_ADDRESS=address)
        encoded = base64.b64encode(_WINDOWS_CERTIFICATE.encode('utf-16-le')).decode('ascii')
        command = ['powershell.exe', '-NoProfile', '-NonInteractive', '-EncodedCommand', encoded]
    else:
        openssl = shutil.which('openssl')
        if not openssl:
            raise ValueError('HTTPS setup requires the installed openssl command on this system')
        environment = os.environ.copy()
        command = [openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1095',
                   '-subj', '/CN=DCO worker', '-addext',
                   'subjectAltName=IP:' + address + ',IP:127.0.0.1,IP:::1,DNS:localhost',
                   '-keyout', str(key), '-out', str(cert)]
    completed = subprocess.run(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if completed.returncode:
        raise ValueError('Certificate creation failed: ' + completed.stderr.decode('utf-8', 'replace')[-1000:])
    if os.name != 'nt':
        key.chmod(0o600)


def configure(root, address, port=26133, *, config_path=None, restart=True):
    """Keep device settings; reload only an idle worker in its existing console.

    The exported profile is private and contains no TLS private key. Copy it
    to the operator PC once; the credential and certificate belong to this
    complete endpoint, independently of the computer's display name.
    """
    address = str(ipaddress.ip_address(address))
    if type(port) is not int or not 0 < port < 65536:
        raise ValueError('Worker port must be between 1 and 65535')
    root = Path(root).resolve()
    path = Path(config_path).resolve() if config_path else root / 'worker.local.json'
    previous = json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else {}
    token = os.environ.get('REMOTE_COMPUTE_TOKEN') or previous.get('token') or secrets.token_urlsafe(32)
    health = None
    live = None
    if restart:
        schemes = ['https', 'http'] if previous.get('certfile') else ['http']
        for scheme in schemes:
            ca = previous.get('certfile') if scheme == 'https' else None
            if ca:
                ca = str((root / ca).resolve())
            candidate = Client(scheme + '://127.0.0.1:' + str(port), token, timeout=2, ca_file=ca, operator=True)
            try:
                health = candidate.health()
            except RemoteError as error:
                if getattr(error, 'status_code', None):
                    raise
                continue
            if health.get('role') != 'worker' or Path(health['tool_root']).resolve() != root:
                raise ValueError('This port belongs to another DCO service; use its project directory and worker port')
            live = candidate
            break
    directory = root / '.local' / 'certs' / ('worker-' + str(port))
    cert = (root / previous['certfile']).resolve() if previous.get('certfile') else directory / 'worker.crt'
    key = (root / previous['keyfile']).resolve() if previous.get('keyfile') else directory / 'worker.key'
    if cert.exists() != key.exists():
        raise ValueError('Certificate/key pair is incomplete: ' + str(cert.parent))
    if not cert.exists():
        _make_certificate(cert, key, address)
    context = ssl.create_default_context(cafile=str(cert))
    context.load_cert_chain(str(cert), str(key))
    decoded = ssl._ssl._test_decode_cert(str(cert))
    addresses = {str(ipaddress.ip_address(value)) for kind, value in decoded.get('subjectAltName', ()) if kind == 'IP Address'}
    if address not in addresses or '127.0.0.1' not in addresses:
        raise ValueError('Existing certificate must cover the public IP and 127.0.0.1; kept the existing certificate and configuration')
    if ssl.cert_time_to_seconds(decoded['notAfter']) <= time.time():
        raise ValueError('Existing certificate has expired: ' + str(cert))
    settings = dict(previous, mode='worker', host='0.0.0.0', port=port,
                    token=token, certfile=str(cert), keyfile=str(key))
    atomic_json(path, settings, private=True)
    public_host = '[' + address + ']' if ':' in address else address
    url = 'https://' + public_host + ':' + str(port)
    pem = cert.read_text(encoding='ascii')
    fingerprint = hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()
    profile = root / '.local' / ('worker-' + str(port) + '.connection.json')
    atomic_json(profile, dict(url=url, node=socket.gethostname(), token=token,
                             certificate_pem=pem, certificate_sha256=fingerprint), private=True)
    result = dict(url=url, config=str(path), connection_profile=str(profile),
                  certificate_sha256=fingerprint, certificate_expires=decoded['notAfter'],
                  status='configured_start_in_visible_console')
    if live is not None:
        if health.get('used_cpus') or health.get('queued') or health.get('active_jobs'):
            result['status'] = 'configured_waiting_for_idle'
            return result
        try:
            live.restart()
        except RemoteError as error:
            if getattr(error, 'status_code', None) == 409:
                result['status'] = 'configured_waiting_for_idle'
                return result
            raise
        secure = Client('https://127.0.0.1:' + str(port), token, ca_file=str(cert), operator=True, timeout=1)
        deadline = time.monotonic() + 30
        while True:
            try:
                ready = secure.health(brief=True)
                if ready.get('role') != 'worker':
                    raise ValueError('Restarted service is not a DCO worker')
                result.update(status='https_verified_in_original_console', node=ready['node'], version=ready['version'])
                return result
            except RemoteError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.25)
    return result
