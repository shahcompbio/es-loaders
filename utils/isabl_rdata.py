import isabl_cli as ii
import pandas as pd
import utils.metadata
import logging
import json
import sys
import os


APP_VERSION = '2.0.0'
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
os.environ["ISABL_API_URL"] = 'https://isabl.shahlab.mskcc.org/api/v1/'
os.environ['ISABL_CLIENT_ID'] = '1'


def is_sample_level_analysis(analysis):
    return analysis['application']['name']        == 'SCRNA' \
           and analysis['application']['version'] == APP_VERSION \
           and analysis['status']                 == 'SUCCEEDED'


def is_individual_level_analysis(analysis):
    return analysis['application']['name']        == 'SCRNA Individual Application' \
           and analysis['application']['version'] == APP_VERSION \
           and analysis['status']                 == 'SUCCEEDED'


def filter_analyses(analyses):
    """
    This function returns a dataframe containing the latest successful analyses details for individuals.
    Latest analyses details are retrieved by identifying the highest primary key for an analyses
    by individual, app & app version.

    For example:
    individual      app         app_version         analysis_pk
    x1             SCRNA           2.0.0              1
    x1             SCRNA           2.0.0              2
    x1             SCRNA           2.0.0              3

    For individual x1 we're interested in analysis_pk = 3 since that's the latest.
    """
    successful_analyses = pd.DataFrame(columns=[
        'individual',
        'analysis_pk',
        'app',
        'app_version',
        'sample_id',
        'nick_sample_id',
        'rdata_path',
        'modified',
    ])

    sample_look_up = get_samples()

    for analysis in analyses:
        row = {}
        if is_sample_level_analysis(analysis):
            row['individual']         = analysis['targets'][0]['sample']['individual']['identifier']
            row['app']                = 'SCRNA'
            row['sample_id']          = analysis['targets'][0]['sample']['identifier']
            try:
                row['nick_sample_id'] = sample_look_up[row['sample_id']]
            except:
                row['nick_sample_id'] = ''
            row['rdata_path']         = analysis['results']['sce']

        elif is_individual_level_analysis(analysis):
            row['individual']     = analysis['individual_level_analysis']['identifier']
            row['app']            = 'SCRNA Individual Application'
            row['sample_id']      = ''  # individual SCRNA doesn't have a sample
            row['nick_sample_id'] = ''  # individual SCRNA doesn't have a sample
            row['rdata_path']     = analysis['results']['scanorama']

        else:
            continue

        row['analysis_pk'] = analysis['pk']
        row['app_version'] = APP_VERSION
        row['modified']    = analysis['modified']
        successful_analyses = successful_analyses.append(row, ignore_index=True)

    # get latest analysis value for a given individual, app & app version (returned as series)
    latest_successful_analyses = successful_analyses.groupby(['individual', 'app'])['analysis_pk'].max()

    # convert series to a dateframe
    latest_successful_analyses = pd.DataFrame(latest_successful_analyses).reset_index()

    # return filtered dataframe containing all columns for latest analysis results
    return latest_successful_analyses.merge(successful_analyses, how='inner', on=['individual', 'app', 'analysis_pk'])


def get_samples():
    samples_metadata = utils.metadata.all_samples()
    sample_look_up = {}

    for key in samples_metadata:
        sample = samples_metadata[key]
        sample_look_up[sample['unique_id']] = sample['nick_unique_id']

    return sample_look_up


def get_scrna_rdata(individuals=None):
    latest_scrna_rdata = []
    filtered_analyses = filter_analyses(ii.get_instances('analyses'))

    if individuals is not None:
        df_individuals = pd.DataFrame(individuals, columns=['individual'])
        filtered_analyses = filtered_analyses.merge(df_individuals, how='inner', on=['individual'])

    for index, df_row in filtered_analyses.iterrows():
        row = {
            'individual':     df_row['individual'],
            'app':            df_row['app'],
            'analysis_pk':    df_row['analysis_pk'],
            'app_version':    df_row['app_version'],
            'sample_id':      df_row['sample_id'],
            'nick_sample_id': df_row['nick_sample_id'],
            'rdata_path':     df_row['rdata_path'],
            'modified':       df_row['modified']
        }
        latest_scrna_rdata.append(row)

    return json.dumps(latest_scrna_rdata)


# get_scrna_rdata()
# get_scrna_rdata(['SPECTRUM-OV-002', 'SPECTRUM-OV-003', 'SPECTRUM-OV-007'])

