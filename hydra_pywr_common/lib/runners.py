import sys
import json
import subprocess

def write_output(text, dest=sys.stdout):
    print(text, file=dest)
    dest.flush()


class IntegratedModelRunner():

    def __init__(self, pynsim_config):
        self.pynsim_config = pynsim_config

    def run_subprocess(self):
        fdf, fdfcmd = "fdf", "run"
        #outfile = "pynsim_config.json"
        #with open(outfile, 'w') as fp:
        #    json.dump(self.pynsim_config, fp)
        pargs = (fdf, fdfcmd, self.pynsim_config)
        write_output(f"Begin model run using: {pargs=}...")
        #proc = subprocess.Popen(pargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc = subprocess.Popen(pargs, stdout=subprocess.PIPE)
        out = proc.communicate()
        write_output("Model run complete")
        """
        write_output(f"Model execution complete with exit code: {proc.returncode}")
        write_output(out.decode())
        write_output(err.decode())
        """


class MultiNetworkRunner():

    def __init__(self, model_config_filename):
        self.model_config_filename = model_config_filename


    def run_subprocess(self, cmd, cmdarg="run" ):
        pargs = (cmd, cmdarg, self.model_config_filename)

        proc = subprocess.Popen(pargs, stdout=subprocess.PIPE)
        out = proc.communicate()
        write_output("Model run complete")
