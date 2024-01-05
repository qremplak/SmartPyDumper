
/opt/conda/envs/uat_env/bin/python -m pip install --upgrade twine
/opt/conda/envs/uat_env/bin/pip install --upgrade keyring keyrings.alt

#/opt/conda/envs/uat_env/bin/python -m twine upload --repository testpypi dist/*
/opt/conda/envs/uat_env/bin/python -m twine upload dist/*