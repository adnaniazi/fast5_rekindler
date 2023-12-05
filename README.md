# fast5_rekindler

[![PyPI](https://img.shields.io/pypi/v/fast5_rekindler?style=flat-square)](https://pypi.python.org/pypi/fast5_rekindler/)
[![PyPi Downloads](https://img.shields.io/pypi/dm/fast5_rekindler)](https://pypistats.org/packages/fast5_rekindler)
[![CI/CD](https://github.com/adnaniazi/fast5_rekindler/actions/workflows/release.yml/badge.svg)](https://github.com/adnaniazi/fast5_rekindler/actions/workflows/release.yml)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fast5_rekindler?style=flat-square)](https://pypi.python.org/pypi/fast5_rekindler/)
[![PyPI - License](https://img.shields.io/pypi/l/fast5_rekindler?style=flat-square)](https://pypi.python.org/pypi/fast5_rekindler/)


---

**Documentation**: [https://adnaniazi.github.io/fast5_rekindler](https://adnaniazi.github.io/fast5_rekindler)

**Source Code**: [https://github.com/adnaniazi/fast5_rekindler](https://github.com/adnaniazi/fast5_rekindler)

**PyPI**: [https://pypi.org/project/fast5_rekindler/](https://pypi.org/project/fast5_rekindler/)

---

Collates information from BAM and POD5 files and generates FAST5 files for use in legacy tools such as tailfindr.

## Installation

1\. Create Python 3.10 or 3.11 environment.

 ```bash
 conda create -n f5r python=3.11
 ```

2\. Activate the environment.

 ```bash
 conda activate f5r
 ```

3\. Install FAST5 Rekindler.

 ```bash
 pip install fast5_rekindler
 ```

## Usage
FAST5 rekindler needs:

1\. A BAM file with `moves` table in it.

You can generate it using Dorado:
```sh
dorado basecaller /path/to/basecalling/model \
  /pod5/dir/path \
  --recursive  \
  --emit-sam  \
  --emit-moves  \
  --device "cpu"  \ # or "cuda:all"
  --reference /path/to/alginment/reference > /path/to/calls.sam
```

2\. Convert Doarado's output SAM file to a BAM file.
```sh
samtools view -bS /path/to/calls.sam > /path/to/calls.bam
```

3\. Sort the BAM file.
```sh
samtools sort /path/to/calls.bam -o /path/to/sorted.calls.bam
```

4\. Use FAST5 Rekindler to convert POD5 files to FAST5 files.

```sh
fast5_rekindler /path/to/sorted.calls.bam  \
  /path/to/pod5_dir \
  /path/to/output_dir \
  --num_processes 100
```

To invoke help for FAST5 Rekindler, just type:

```sh
fast5_rekindler --help
```

## Development

* Clone this repository
* Requirements:
  * [Poetry](https://python-poetry.org/)
  * Python 3.7+
* Create a virtual environment and install the dependencies

```sh
poetry install
```

* Activate the virtual environment

```sh
poetry shell
```

### Testing

```sh
pytest
```

### Documentation

The documentation is automatically generated from the content of the [docs directory](./docs) and from the docstrings
 of the public signatures of the source code. The documentation is updated and published as a [Github project page
 ](https://pages.github.com/) automatically as part each release.

### Releasing

Trigger the [Draft release workflow](https://github.com/adnaniazi/fast5_rekindler/actions/workflows/draft_release.yml)
(press _Run workflow_). This will update the changelog & version and create a GitHub release which is in _Draft_ state.

Find the draft release from the
[GitHub releases](https://github.com/adnaniazi/fast5_rekindler/releases) and publish it. When
 a release is published, it'll trigger [release](https://github.com/adnaniazi/fast5_rekindler/blob/master/.github/workflows/release.yml) workflow which creates PyPI
 release and deploys updated documentation.

### Pre-commit

Pre-commit hooks run all the auto-formatters (e.g. `black`, `isort`), linters (e.g. `mypy`, `flake8`), and other quality
 checks to make sure the changeset is in good shape before a commit/push happens.

You can install the hooks with (runs for each commit):

```sh
pre-commit install
```

Or if you want them to run only for each push:

```sh
pre-commit install -t pre-push
```

Or if you want e.g. want to run all checks manually for all files:

```sh
pre-commit run --all-files
```
