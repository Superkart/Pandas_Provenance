import os
from setuptools import setup, find_packages

setup(
    name="pandas-provenance",
    version="0.1.0",
    description="Track and trace data provenance for pandas DataFrames - log all transformations for reproducibility and auditability",
    author="Data Provenance Team",
    author_email="support@pandas-provenance.dev",
    url="https://github.com/yourusername/Pandas_Provenance",
    packages=find_packages(where="Pandas_Provenance_Project/src"),
    package_dir={"": "Pandas_Provenance_Project/src"},
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.19.0",
    ],
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
    ],
    keywords="provenance, data-lineage, pandas, reproducibility, audit",
)
