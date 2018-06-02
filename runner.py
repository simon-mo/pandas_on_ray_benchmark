import click
import time
import json
import os
import sys


#utils
def wait(df):
    if hasattr(df, "_block_partitions"):
        blocks = list(df._block_partitions.flatten())
        completed = ray.wait(blocks, num_returns=len(blocks))  #blocking


def prof(df, expr):
    start = time.time()
    exec(expr)
    end = time.time()
    duration_sec = end - start
    return duration_sec


#https://stackoverflow.com/questions/12437667/how-to-replace-punctuation-in-a-string-python
import string
import re
all_punc = re.compile('[%s]' % re.escape(string.punctuation))


@click.command()
@click.option('--runner', type=click.Choice(['ray', 'pandas']), prompt=True)
@click.option('--data', type=click.Path(), prompt=True)
@click.option('--expr', type=str, prompt=True)
@click.option('--num-cpus', type=int, prompt=True)
@click.option('--log-dir', type=click.Path(), default='.', prompt=True)
@click.option('--force', is_flag=True)
def run(runner, data, expr, num_cpus, log_dir, force):
    log_file_name = all_punc.sub(
        '_', f'{runner}_{data}_{expr}_{num_cpus}') + '.json'

    log_path = f"{log_dir}/{log_file_name}"

    if os.path.exists(log_path) and not force:
        click.echo("File exists, skipping")
        sys.exit(1)
        
    os.environ['PD_ON_RAY_CPU'] = str(num_cpus)

    if runner == 'ray':
        import ray
        ray.init(num_cpus=num_cpus)
        import ray.dataframe as pd
    else:  #pandas
        import pandas as pd

    df = pd.read_parquet(data)

    result = {
        'runner': runner,
        'data': data,
        'expr': expr,
        'num_cpus': num_cpus,
        'result': prof(df, expr)
    }

    with open(log_path, 'w') as f:
        json.dump(result, f)
        f.write('\n')
        
run()