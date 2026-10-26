import shlex

import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab

pc = portal.Context()

pc.defineParameter(
    "hwtype",
    "Hardware type",
    portal.ParameterType.NODETYPE,
    "c220g5",
    longDescription="Node type to use. Must exist at the cluster you pick.",
)

pc.defineParameter(
    "setupUser",
    "Login user for experiment setup",
    portal.ParameterType.STRING,
    "awitt",
    longDescription="CloudLab login account that will own the checkout, drivers, and TPC-H data.",
)

params = pc.bindParameters()

request = pc.makeRequestRSpec()

node_0 = request.RawPC("node-0")
node_0.hardware_type = params.hwtype
node_0.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU24-64-STD"

setup = """set -euo pipefail
exec > "$HOME/experiment-setup.log" 2>&1
setup_script=$(mktemp)
trap 'rm -f "$setup_script"' EXIT
curl -fsSL --retry 3 \\
    https://raw.githubusercontent.com/awitten1/duckdb-adbc-client/feat/clickhouse-partial-sort/scripts/setup_ubuntu_experiments.sh \\
    -o "$setup_script"
bash "$setup_script"
"""
node_0.addService(
    pg.Execute(
        shell="bash",
        command=(
            "set -euo pipefail; "
            "sudo apt-get update; "
            "sudo apt-get install -y curl ca-certificates; "
            "sudo -H -u "
            + shlex.quote(params.setupUser)
            + " bash -c "
            + shlex.quote(setup)
        ),
    )
)

pc.printRequestRSpec(request)
