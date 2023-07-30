from pathlib import Path
from setuptools import find_packages, setup

src = Path(__file__).parent

setup(
    name="reata",
    version="1.1.4",
    description=(
        "A simple MySQL DBAPI wrapper for simplifying "
        "data processing pipelines."
    ),
    long_description=(src/"README.rst").read_text(),
    packages=find_packages(),
    install_requires=[
        "numpy>=1.22.3",
        "pandas>=1.4.2",
        "pdxtra>=1.0.0",
        "mysql-connector-python>=8.0.31",
    ],
    python_requires=">=3.10.4",
    license=(src/"LICENSE").read_text(),
    include_package_data=True,
)
