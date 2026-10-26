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

setup = (
    "exec bash /local/repository/scripts/setup_ubuntu_experiments.sh "
    '> "$HOME/experiment-setup.log" 2>&1'
)
node_0.addService(
    pg.Execute(
        shell="bash",
        command=(
            "sudo -H -u "
            + shlex.quote(params.setupUser)
            + " bash -c "
            + shlex.quote(setup)
        ),
    )
)

pc.printRequestRSpec(request)
