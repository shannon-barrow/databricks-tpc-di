import subprocess, shlex
from . helpers import copy_directory

def DIGen(jar_path, scale_factor, repo_path, tmp_location, output_path):
    #cmd = "java -jar /dbfs/tmp/datagen/DIGen.jar -sf 1000 -o /dbfs/tmp/tpc-di/1000"
    cmd = f"java -jar {jar_path} -sf {scale_factor} -o {output_path}/{scale_factor}"
    print(f"Generating data and outputting to {output_path}/{scale_factor}")
    args = shlex.split(cmd)
    p3 = subprocess.Popen(args,
                     cwd='/dbfs/tmp/datagen',
                     #shell=True,
                     universal_newlines=True,
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
    p3.stdin.write("\n")
    p3.stdin.flush()
    p3.stdin.write("YES\n")
    p3.stdin.flush()
    while True:
        output = p3.stdout.readline()
        if p3.poll() is not None and output == '':
            break
        if output:
            print (output.strip())
    p3.wait()