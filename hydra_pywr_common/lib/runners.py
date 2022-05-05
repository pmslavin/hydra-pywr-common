import json
import os
import subprocess
import sys

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
        proc_env = os.environ.copy()
        proc_env["PYTHONPATH"] = ".:/app:" + proc_env["PYTHONPATH"]
        pargs = (fdf, fdfcmd, self.pynsim_config)
        write_output(f"Begin model run using: {pargs=}...")
        write_output(f"Python env: {proc_env['PYTHONPATH']}...")
        #proc = subprocess.Popen(pargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc = subprocess.Popen(pargs, stdout=subprocess.PIPE, env=proc_env)
        out = proc.communicate()
        write_output("Model run complete")
        """
        write_output(f"Model execution complete with exit code: {proc.returncode}")
        write_output(out.decode())
        write_output(err.decode())
        """
