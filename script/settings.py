import os.path

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../promviz/bin/config.properties')) as f:
    data = f.readlines()

for ln in data:
    ln = ln.strip()
    if not ln or ln.startswith('#'):
        continue

    key, value = ln.split('=')
    key = key.strip()
    value = value.strip()

    if key.startswith('dir_') and not key.endswith('_root'):
        value = os.path.join(dir_root, value)

    globals()[key] = value
