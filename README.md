# sql-transpiler
SQLGlot based transpiler control and evaulation framework.
The config defaults to source = tsql, target = databricks

## Setup

```bash
git clone ....
mkdir resources
<copy your sql files into resources folder>

#setup dev environment
make install-dev
```

## Unit Testing
```bash
make test
```

## Before commit
```bash
make check
```

## Usage
```bash
pip install sqlglot
python3 control.py
```