# project/src/__init__.py
import os
import sys

# Add project root directory to sys.path
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
project_root = os.path.abspath(os.path.join('..'))
sys.path.append(project_root)

# Add src and config directories to sys.path
src_dir = os.path.join(project_root, 'src')
config_dir = os.path.join(project_root, 'config')
scripts_dir = os.path.join(project_root, 'scripts')
sys.path.extend([src_dir, config_dir, scripts_dir])

