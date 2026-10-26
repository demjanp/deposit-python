# <img src="dep_cube.svg" width="32">Deposit Python

Synchronous Python client for Deposit Server 2. The distribution and import are
named `deposit`; the 2.x generation is distinct from Deposit 1.x and does not
promise API compatibility. Applications that use both generations should use
separate environments.

This repository is part of the [Deposit Hub](https://github.com/demjanp/deposit-hub).

Deposit was created on 29. 4. 2013

<details>
<summary>Table of Contents</summary>

1. [About Deposit](#about)
2. [Installation](#installation)
3. [Contact](#contact)
4. [Acknowledgements](#acknowledgements)
5. [License](#license)

</details>

## About Deposit <a name="about"></a>
Deposit is open source graph database engine that serves as a storage and management platform for both the collection as well as analysis of scientific data.

Deposit also has a [GUI frontend](https://github.com/demjanp/deposit_gui)

It serves as a backend for the following applications:
* [Laser Aided Profiler](https://www.laseraidedprofiler.com/)
* [CeraMatch](https://github.com/demjanp/CeraMatch)


## Installation <a name="installation"></a>

Planned release installation commands (available after Stage 6 and publication):
```
pip install "deposit>=2,<3"
```

NetworkX graph conversion is planned for Stage 6; its optional extra will be:

```
pip install "deposit[networkx]>=2,<3"
```

The initial integration support matrix targets Windows x64, Python 3.14.x,
Neo4j Python driver 6.x, and the exact Deposit Server build recorded in the
installed package's procedure manifest. Unsupported server builds fail before
Deposit API calls.

For the GUI installer see [Deposit GUI releases](https://github.com/demjanp/deposit_gui/releases/latest)

## Contact: <a name="contact"></a>
Peter Demján (peter.demjan@gmail.com)

Institute of Archaeology of the Czech Academy of Sciences, Prague, v.v.i.

## Acknowledgements <a name="acknowledgements"></a>

Development of this software was supported by OP RDE, MEYS, under the project "Ultra-trace isotope research in social and environmental studies using accelerator mass spectrometry", Reg. No. CZ.02.1.01/0.0/0.0/16_019/0000728.

This software uses the following open source packages:
* [Neo4j](https://github.com/neo4j/neo4j-python-driver)

## License <a name="license"></a>

This code is licensed under the [GNU GENERAL PUBLIC LICENSE](https://www.gnu.org/licenses/gpl-3.0.en.html) - see the [LICENSE](LICENSE) file for details
