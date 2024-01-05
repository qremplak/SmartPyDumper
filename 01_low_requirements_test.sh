#> Create lowest Python-version venv
#> Install lowest requirements from pyproject.toml
#> Unit test local dev code with pytest

conda env remove --name low_env
conda create -y -n low_env python=3.8
/opt/conda/envs/low_env/bin/pip install pytest
/opt/conda/envs/low_env/bin/pip install $(perl -ane 'print "$1 " while /"(.+\>.+)"/g' ./pyproject.toml | tr '>' '=')
/opt/conda/envs/low_env/bin/pip list
/opt/conda/envs/low_env/bin/python -m pytest -k 'test_' .

## Adjust lower bounds of dependencies in pyproject.toml according to results