# sql-transpiler
[SQLGlot](https://github.com/tobymao/sqlglot) based transpiler control and evaulation framework. This tool is useful in looking at an entire migration project and where sqlglot optimizations are required.
The config defaults to source = tsql, target = databricks
The control.py program stores the per statement parsing results into a delta table. The sqlglot evaluate notebook helps you summarize the results and debug individual transpilation issues.

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
make install
python3 control.py
```

## Review Evaluation
Open the `03 - sqlglot-evaluate` notebook to analyze the results stored in the delta table

## More Resources
https://github.com/tobymao/sqlglot
