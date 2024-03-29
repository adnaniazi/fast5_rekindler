# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.6] - 2024-01-12
### Fixed
- Removed redundant code that gave error when closing the database

## [0.0.5] - 2024-01-12
### Fixed
- A bug where some processes created emtpy tmp_worker files that caused problems during merging them in bam.db

## [0.0.4] - 2023-12-13
### Fixed
- A bug that caused tailfindr to crash because the basecalling subgroup name had a typo

## [0.0.3] - 2023-12-05
### Added
- Splitting of duplex reads in FAST5

### Fixed
- Errors encountered in suplementary alignments due to missing FASTA data
- CLI text
- Fixed missing BAM sorting step

## [0.0.2] - 2023-11-16
### Added
- First fully functional release of code which can collate info in POD5 and BAM file into legacy basecalled FAST5 files

[Unreleased]: https://github.com/adnaniazi/fast5_rekindler/compare/0.0.6...master
[0.0.6]: https://github.com/adnaniazi/fast5_rekindler/compare/0.0.5...0.0.6
[0.0.5]: https://github.com/adnaniazi/fast5_rekindler/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/adnaniazi/fast5_rekindler/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/adnaniazi/fast5_rekindler/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/adnaniazi/fast5_rekindler/tree/0.0.2

