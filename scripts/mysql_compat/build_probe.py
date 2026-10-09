#!/usr/bin/env python3
"""Build only the standalone probe; first build libsqlparser.a with make -B lib."""
import argparse
import os
from pathlib import Path
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(shlex.split(os.environ.get('CXX', 'c++')) + [
        '-std=c++17', '-O2', '-Wall', '-Wextra', '-I' + str(ROOT / 'include'),
        str(Path(__file__).with_name('probe.cpp')), str(ROOT / 'libsqlparser.a'),
        '-pthread', '-o', str(args.output)], check=True)


if __name__ == '__main__':
    main()
