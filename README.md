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
<img width="871" alt="image" src="https://github.com/dmoore247/sql-transpiler/assets/1122251/bae84089-82f5-4252-9d42-f6e7fa95e6ac">


<img width="1306" alt="image" src="https://github.com/dmoore247/sql-transpiler/assets/1122251/c6f399e2-7862-47d7-81a9-6edcc242ee0e">


## More Resources
https://github.com/tobymao/sqlglot
