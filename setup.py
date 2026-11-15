# Lets Dataflow workers import our packages (pipelines/, simulator/).
import setuptools

setuptools.setup(name="parkflow", version="0.1.0", packages=setuptools.find_packages(exclude=["tests"]))
