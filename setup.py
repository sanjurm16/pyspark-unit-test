from setuptools import setup, find_packages
setup(
    name="Pyspark-Test",
    version="0.1",
    scripts=['src/main.py'],
    install_requires=['pyspark'],
    packages=['src'],
    author="Sanjay Mishra",
    author_email="admin@devrats.com",
    description="This is an example spark unit-test",
    keywords="pyspark unit test",
    url="http://devrats.com",   # project home page, if any
    project_urls={
        "Source Code": "https://github.com/sanjurm16/pyspark-unit-test"
    }
)