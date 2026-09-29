"""macOS opt-in wheel install and tokenizer use under OS network denial."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def command(args, *, env=None, cwd=None, timeout=180):
    subprocess.run(args, check=True, env=env, cwd=cwd, timeout=timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    wheels = root / 'wheels'
    wheels.mkdir()
    cache = root / 'token-cache'
    cache.mkdir()
    repo = Path(__file__).resolve().parents[2]
    if sys.version_info[:2] != (3, 13):
        raise RuntimeError('Python 3.13 required')
    sandbox = ['/usr/bin/sandbox-exec', '-p', '(version 1)(allow default)(deny network*)']
    env = {**os.environ, 'PIP_DISABLE_PIP_VERSION_CHECK': '1', 'TIKTOKEN_CACHE_DIR': str(cache)}
    env.pop('PYTHONPATH', None)
    # This preparation phase intentionally has network access.
    command([sys.executable, '-m', 'pip', 'wheel', '--wheel-dir', str(wheels), str(repo), 'tiktoken==0.14.0'], env=env)
    command([sys.executable, '-c', "import tiktoken; [tiktoken.get_encoding(n) for n in ('cl100k_base','o200k_base')]"], env=env)
    manifest = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for folder in (wheels, cache) for p in folder.iterdir() if p.is_file()}
    (root / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    command([sys.executable, '-m', 'venv', str(root / 'venv')])
    python = str(root / 'venv/bin/python')
    core = tuple(wheels.glob('agent_memory-*.whl'))
    if len(core) != 1:
        raise RuntimeError('Expected one agent_memory core wheel')
    # Every following install/probe is executed with OS-level network denial.
    install = sandbox + [python, '-m', 'pip', 'install', '--no-index', '--no-cache-dir', '--find-links', str(wheels)]
    command(install + [str(core[0])], env=env, cwd=root)
    core_probe = """
import errno,socket,sys
from agent_memory.model_token_counter import TiktokenModelCounter
assert sys.version_info[:2] == (3,13)
s=socket.socket()
try:
    try: s.connect(('127.0.0.1',9))
    except OSError as error: assert error.errno in (errno.EPERM,errno.EACCES), repr(error)
    else: raise AssertionError('network sandbox not active')
finally: s.close()
try:
    TiktokenModelCounter(encoding_name='cl100k_base',model_id='offline-test',template_version='v1')
except ImportError:
    print('CORE_WITHOUT_OPTIONAL_DEPENDENCY_OK')
else:
    raise AssertionError('optional dependency unexpectedly present')
"""
    command(sandbox+[python,'-c',core_probe],env=env,cwd=root)
    command(install+['tiktoken==0.14.0'],env=env,cwd=root)
    probe = """
import tiktoken
from agent_memory.model_token_counter import TiktokenModelCounter
for name in ('cl100k_base','o200k_base'):
    counter=TiktokenModelCounter(encoding_name=name,model_id='offline-test',template_version='v1',framing_tokens=5,reserve_tokens=7)
    for text in ('hello world','\u4f60\u597d\uff0c\u8bb0\u5fc6','<|endoftext|>','a\\n\\tb'):
        expected=len(tiktoken.get_encoding(name).encode(text,disallowed_special=()))+12
        assert counter.count(text)==expected
print('OFFLINE_CACHED_TOKENIZATION_OK')
"""
    command(sandbox+[python,'-c',probe],env=env,cwd=root)
    command(sandbox+[python,'-c',probe],env=env,cwd=root)
    empty=root/'empty-cache'
    empty.mkdir()
    negative="""
from agent_memory.model_token_counter import TiktokenModelCounter
try:
    TiktokenModelCounter(encoding_name='cl100k_base',model_id='offline-test',template_version='v1')
except (OSError,RuntimeError):
    print('MISSING_CACHE_FAILS_CLOSED')
else:
    raise AssertionError('uncached tokenizer unexpectedly succeeded offline')
"""
    command(sandbox+[python,'-c',negative],env={**env,'TIKTOKEN_CACHE_DIR':str(empty)},cwd=root,timeout=30)
    report={'status':'passed','python':'3.13','network_isolation':'macOS sandbox deny network*',
            'encodings':['cl100k_base','o200k_base'],'fresh_process_checks':2,
            'wheel_install':'no-index local wheelhouse','missing_dependency':'explicit ImportError',
            'missing_cache':'fails closed','manifest':str(root/'manifest.json'),
            'boundary':'current macOS architecture, locally built workspace wheel; not all deployment platforms'}
    (root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


if __name__=='__main__':
    main()
