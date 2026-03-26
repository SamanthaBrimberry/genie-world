---
sidebar_position: 1
title: Installation
---

# Installation

## Prerequisites

- Python 3.10+
- A Databricks workspace with a SQL warehouse
- Databricks CLI or SDK authentication configured

## Install from GitHub

```bash
pip install git+https://github.com/SamanthaBrimberry/genie-world.git
```

## Install for Development

Clone the repo and install in editable mode:

```bash
git clone https://github.com/SamanthaBrimberry/genie-world.git
cd genie-world
pip install -e ".[all]"
```

## Install on Databricks

In a Databricks notebook:

```python
%pip install git+https://github.com/SamanthaBrimberry/genie-world.git
dbutils.library.restartPython()
```

## Verify Installation

```python
import genie_world
print(genie_world.__version__)
```
