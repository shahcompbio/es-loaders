# File transfer script to only be used on juno

import subprocess
import os
import json
from tinydb import TinyDB, Query

from mira.isabl_rdata import get_scrna_rdata


def transfer_all_new():
    samples = get_all_samples()
    updated_samples = get_updated_samples(samples)

    # transfer appropriate files to spectrum-loader
    print('{} samples will be transferred'.format(str(len(updated_samples))))
    transferred_samples = []
    for sample in updated_samples:
        rdata_path = sample['rdata_path']
        dashboard_id = sample['dashboard_id']
        cmd = f'rsync -a {rdata_path} spectrum-loader:{dashboard_id}.rdata'

        process = subprocess.Popen(['/bin/bash', '-c', cmd])
        stdoutd, stderrd = process.communicate()
        if process.returncode == 0:
            transferred_samples.append(sample)

    # generate and transfer 'manifest' list of moved files
    print('Creating manifest with {} samples'.format(
        str(len(transferred_samples))))
    if os.path.exists('transferred.json'):
        os.remove('transferred.json')
    with open('transferred.json', 'w') as file:
        file.write(json.dumps(transferred_samples))

    print('Moving manifest')
    cmd = f'rsync -a transferred.json spectrum-loader:transferred.json'
    subprocess.Popen(['/bin/bash', '-c', cmd])

    # move all files
    print('Moving files')
    subprocess.Popen(
        ['/bin/bash', '-c', 'ssh spectrum-loader sudo mv *.rdata /dat/mira/.'])

    # update appropriate records in db
    print('Update local db')
    update_samples_in_db(transferred_samples)


def get_all_samples():
    samples = get_scrna_rdata()
    return [_get_record(row) for row in samples]


def _get_record(row):
    return {
        'dashboard_id': row['individual'] if row['sample_id'] == "" else row['nick_sample_id'],
        'type': 'patient' if row['sample_id'] == "" else 'sample',
        'analysis_pk': row['analysis_pk'],
        'app_version': row['app_version'],
        'modified': row['modified'],
        'rdata_path': row['rdata_path']
    }


def get_all_transfered_samples():
    db = TinyDB('db.json')
    return db.all()


def get_updated_samples(isabl_samples):
    db = TinyDB('db.json')
    Dashboard = Query()

    updated_samples = []
    for sample in isabl_samples:
        searched = db.search(Dashboard.dashboard_id == sample['dashboard_id'])

        if len(searched) == 0:
            updated_samples.append(sample)

        elif searched[0]['modified'] < sample['modified']:
            updated_samples.append(sample)

    return updated_samples


def update_samples_in_db(samples):
    db = TinyDB('db.json')
    Dashboard = Query()

    for sample in samples:
        db.upsert(sample, Dashboard.dashboard_id == sample['dashboard_id'])


if __name__ == '__main__':
    transfer_all_new()
