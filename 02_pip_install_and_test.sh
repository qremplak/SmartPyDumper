#> Create recent Python venv
#> pip install local code with dependencies
#> Unit test installed module with pytest

conda env remove --name uat_env
conda create -y -n uat_env python=3.11
/opt/conda/envs/uat_env/bin/pip install pytest
/opt/conda/envs/uat_env/bin/pip install --upgrade --force-reinstall install .
/opt/conda/envs/uat_env/bin/pip list
/opt/conda/envs/uat_env/bin/python -m pytest -k 'test_' .