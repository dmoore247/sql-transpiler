from setuptools import find_packages, setup

setup(
    name="sql_transpiler",
    description="",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/databricks/sql-transpiler",
    author="Douglas Moore",
    author_email="douglas.moore@databricks.com",
    license="Apache 2",
    packages=find_packages(include=["databricks-sdk", "databricks-connect"]),
    package_data={"ucrolehelper": ["py.typed"]},
    use_scm_version={
        "write_to": "version.py",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    setup_requires=["setuptools_scm"],
    install_requires=["sqlglot", "databricks-connect"],
    extras_require={
        "dev": [
            "autoflake",
            "black",
            "isort",
            "mypy>=0.990",
            "pdoc",
            "pre-commit",
        ],
    },
    classifiers=[
        "Development Status :: 1 - x/y",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
