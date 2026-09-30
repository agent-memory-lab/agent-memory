"""Local deployment contract tests; not certification of a production gateway."""
import asyncio
from datetime import UTC, datetime, timedelta
import ipaddress
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import sys
import time

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from agent_memory_sdk import MCPMemoryClient


@pytest.fixture(scope='module')
def tls_gateway(tmp_path_factory):
    root = tmp_path_factory.mktemp('local-tls-gateway')
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'Local acceptance CA')])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(ca_key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=5))
          .not_valid_after(now+timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
          .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=False,
                                      data_encipherment=False, key_agreement=False, key_cert_sign=True,
                                      crl_sign=True, encipher_only=None, decipher_only=None), critical=True)
          .sign(ca_key, hashes.SHA256()))
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cert = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')]))
            .issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now-timedelta(minutes=5)).not_valid_after(now+timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=True,
                                        data_encipherment=False, key_agreement=False, key_cert_sign=False,
                                        crl_sign=False, encipher_only=None, decipher_only=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'), x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    (root/'ca.pem').write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (root/'server.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (root/'server.key').write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    os.chmod(root/'server.key', 0o600)
    bearer = secrets.token_urlsafe(32)
    (root/'gateway.json').write_text(json.dumps({'bearer': bearer, 'signing_secret': secrets.token_hex(32)}))
    os.chmod(root/'gateway.json', 0o600)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    context = ssl.create_default_context(cafile=str(root/'ca.pem'))
    url = f'https://127.0.0.1:{port}/mcp'
    with (root/'server.log').open('w') as log:
        process = subprocess.Popen([sys.executable, str(Path(__file__).with_name('tls_gateway_fixture.py')), str(root), str(port)], stdout=log, stderr=log)
        try:
            deadline = time.monotonic()+10
            with httpx.Client(verify=context, trust_env=False, timeout=.3) as client:
                while True:
                    assert process.poll() is None, 'TLS fixture startup failed'
                    try:
                        assert client.get(url).status_code == 401
                        break
                    except httpx.TransportError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('TLS fixture startup timeout')
                        time.sleep(.05)
            yield url, context, bearer, port
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def test_tls_trusted_certificate_and_mcp_round_trip(tls_gateway):
    url, context, bearer, _ = tls_gateway
    async def exercise():
        async with httpx.AsyncClient(verify=context, trust_env=False, headers={'Authorization': 'Bearer '+bearer}) as http:
            async with MCPMemoryClient.from_http(url, http_client=http) as client:
                assert 'result' in await client.recovery_stats()
    asyncio.run(exercise())


def test_tls_rejects_untrusted_ca(tls_gateway):
    url, _, _, _ = tls_gateway
    with httpx.Client(trust_env=False, timeout=2) as client:
        with pytest.raises(httpx.ConnectError):
            client.get(url)


def test_tls_rejects_wrong_hostname(tls_gateway):
    _, context, _, port = tls_gateway
    with socket.create_connection(('127.0.0.1', port), timeout=2) as raw:
        with pytest.raises(ssl.SSLCertVerificationError):
            with context.wrap_socket(raw, server_hostname='wrong.example.invalid'):
                pass


@pytest.mark.parametrize('authorization', [None, 'Bearer invalid'])
def test_gateway_rejects_missing_or_invalid_bearer(tls_gateway, authorization):
    url, context, _, _ = tls_gateway
    with httpx.Client(verify=context, trust_env=False, timeout=2) as client:
        response = client.post(url, headers={} if authorization is None else {'Authorization': authorization}, json={})
        assert response.status_code == 401


def test_gateway_discards_spoofed_identity(tls_gateway):
    url, context, bearer, _ = tls_gateway
    async def exercise():
        headers = {'Authorization': 'Bearer '+bearer, 'x-agent-memory-tenant-id': 'attacker',
                   'x-agent-memory-signature': 'forged', 'x-agent-memory-can-erase': 'true', 'x-forwarded-for': '203.0.113.10'}
        async with httpx.AsyncClient(verify=context, trust_env=False, headers=headers) as http:
            async with MCPMemoryClient.from_http(url, http_client=http) as client:
                assert 'result' in await client.recovery_stats()
    asyncio.run(exercise())
