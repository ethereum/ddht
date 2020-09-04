#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import (
    setup,
    find_packages,
)

extras_require = {
    "test": [
        "factory-boy==2.12.0",
        "hypothesis>=4.45.1,<5",
        "pexpect==4.8.0",
        "pytest==6.0.1",
        "pytest-randomly>=3.1.0,<3.2",
        "pytest-trio>=0.6.0,<0.7",
        "pytest-xdist==2.0.0",
        "tox==3.19.0",
    ],
    "lint": [
        "black==19.10b0",
        "flake8==3.8.3",
        "isort>=5.1.4,<6",
        "mypy==0.782",
        "pydocstyle>=3.0.0,<4",
    ],
    "doc": ["Sphinx>=1.6.5,<2", "sphinx_rtd_theme>=0.1.9", "towncrier>=19.2.0, <20"],
    "dev": [
        "bumpversion>=0.5.3,<1",
        "pytest-watch>=4.1.0,<5",
        "wheel",
        "twine",
        "ipython",
    ],
}

extras_require["dev"] = (
    extras_require["dev"]
    + extras_require["test"]  # noqa: W504
    + extras_require["lint"]  # noqa: W504
    + extras_require["doc"]  # noqa: W504
)


with open("./README.md") as readme:
    long_description = readme.read()


setup(
    name="ddht",
    # *IMPORTANT*: Don't manually change the version here. Use `make bump`, as described in readme
    version="0.1.0-alpha.1",
    description="""ddht: Implementation of the P2P Discoveryv5 Protocol""",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="The Ethereum Foundation",
    author_email="snakecharmers@ethereum.org",
    url="https://github.com/ethereum/ddht",
    include_package_data=True,
    install_requires=[
        "async-service>=0.1.0a9,<0.2",
        "cached-property>=1.5.1,<2",
        "coincurve>=10.0.0,<11.0.0",
        "cryptography>=3.0,<3.2",
        "eth-enr>=0.3.0,<0.4",
        "eth-hash[pycryptodome]>=0.1.4,<1",
        "eth-keys>=0.3.3,<0.4.0",
        "eth-typing>=2.2.2,<3",
        "eth-utils>=1.8.4,<2",
        "lru-dict>=1.1.6,<2.0",
        "plyvel>=1.2.0,<1.3",
        "py-evm==0.3.0a19",
        "rlp>=2.0.0a1,<3.0.0",
        "trio>=0.16.0,<0.17",
        "trio-typing>=0.5.0,<0.6",
        "upnp-port-forward>=0.1.1,<0.2",
    ],
    python_requires=">=3.8, <4",
    extras_require=extras_require,
    py_modules=["ddht"],
    license="MIT",
    zip_safe=False,
    keywords="ethereum",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    entry_points={"console_scripts": ["ddht=ddht._boot:_boot"]},
)
