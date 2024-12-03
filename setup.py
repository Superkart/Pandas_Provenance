from setuptools import setup, find_packages

setup(
    name="PandasProvenance",
    version="1.0.0",
    description="Provenance tracking for pandas DataFrames",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pandas",  # Add other dependencies here
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
