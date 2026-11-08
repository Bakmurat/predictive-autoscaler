import os
import sys

os.environ["KUBECONFIG"] = "/dev/null"          # nothing here may ever reach a cluster
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
