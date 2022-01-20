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
        outfile = "pynsim_config.json"
        with open(outfile, 'w') as fp:
            json.dump(self.pynsim_config, fp)
        pargs = (fdf, fdfcmd, outfile)
        write_output(f"Begin model run using: {pargs=}...")
        proc = subprocess.Popen(pargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = proc.communicate()

        write_output(f"Model execution complete with exit code: {proc.returncode}")
        write_output(out.decode())
        write_output(err.decode())
