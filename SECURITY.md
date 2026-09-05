# Security Policy

## Supported versions

Security fixes are provided for the latest minor release on the `0.1.x` line while the
project remains pre-1.0.

## Reporting a vulnerability

Do not open a public issue for suspected tenant-isolation, authorization, deletion,
prompt-injection, credential, or data-exposure vulnerabilities. Use GitHub private
vulnerability reporting after the repository is published. Include affected versions,
configuration, reproduction steps, impact, and any proposed mitigation.

Until a private reporting channel is configured, do not deploy this project with
sensitive production data.

## Security boundaries

- Model-generated arguments are untrusted.
- Tenant and scope identity must come from the host or an authenticated gateway.
- Streamable HTTP must not be exposed without a trusted identity resolver.
- Legal erase requires a separately authorized request context.
- Generated Procedures remain candidates until evaluation and promotion gates pass.
- Secrets must be supplied through environment or platform secret stores, never memory
  events, plugin manifests, source files, or command-line examples committed to Git.

See [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md) for the complete model.

