import os
from setuptools import setup, find_packages

README_PATH = "README.md"
if os.path.exists(README_PATH):
    with open(README_PATH, "r", encoding="utf-8") as readme_file:
        LONG_DESCRIPTION = readme_file.read()
else:
    LONG_DESCRIPTION = ""

setup(
    name="pandas-provenance",
    version="0.1.0",
    description="Track and trace data provenance for pandas DataFrames - log all transformations for reproducibility and auditability",
    author="Data Provenance Team",
    packages=find_packages(where="Pandas_Provenance_Project/src"),
    package_dir={"": "Pandas_Provenance_Project/src"},
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.19.0",
    ],
    long_description=LONG_DESCRIPTION,
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
