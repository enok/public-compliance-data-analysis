---
title: Notebook hardcoded AWS credentials must use runtime_config.json
category: security
created: 2026-04-08
tags: [notebook, aws, credentials, security, runtime-config, s3]
---

# Problem

Jupyter notebooks had hardcoded AWS S3 bucket names and profile names directly in code cells. This is a security risk — credentials leak into committed notebook outputs and are not portable across environments.

# Failed Approaches

1. Directly editing `.ipynb` files with the `edit` tool — `.ipynb` files are JSON and cannot be edited with standard text-replace tools in most IDEs.

# Solution

Use a Python script to programmatically modify notebook JSON cells. Replace hardcoded values with a config-loading cell pattern:

```python
import json as _json

_rtcfg_path = os.path.join('..', 'config', 'runtime_config.json')
if os.path.exists(_rtcfg_path):
    with open(_rtcfg_path) as _f:
        _rtcfg = _json.load(_f)
else:
    _rtcfg = {}

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', _rtcfg.get('aws', {}).get('s3_bucket_name', ''))
AWS_PROFILE = os.environ.get('AWS_PROFILE', _rtcfg.get('aws', {}).get('profile', None))
```

Then reference `S3_BUCKET_NAME` and `AWS_PROFILE` throughout the notebook instead of hardcoded strings.

**Critical:** Never add `print()` statements for `S3_BUCKET_NAME` or `AWS_PROFILE` — they leak into committed notebook outputs.

# Why

Environment-specific values must live in configuration files or environment variables, not in source code. The `config/runtime_config.json` file already exists for this purpose. Notebooks are particularly dangerous because their outputs (including printed secrets) get committed to version control.
