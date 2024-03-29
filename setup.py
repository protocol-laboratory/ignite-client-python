from setuptools import setup, find_packages

setup(
    name="ignite_client",
    version="0.0.1",
    author="shoothzj",
    author_email="shoothzj@gmail.com",
    description="A Python package for interacting with the Apache Ignite",
    long_description_content_type="text/markdown",
    url="https://github.com/protocol-laboratory/ignite-client-python",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
