import setuptools  # type: ignore

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="chouette-iot",
    version="0.0.1b",
    author="Artem Katashev",
    author_email="aharr@rowanleaf.net",
    description="Monitoring and metrics collecting Datadog integration for IoT devices",
    license="Apache License, Version 2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/akatashev/chouette-iot",
    packages=setuptools.find_packages(),
    install_requires=[
        "redis",
        "pykka",
        "requests",
        "requests-unixsocket",
        "python-json-logger",
        "psutil",
        "pydantic",
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.6",
)
