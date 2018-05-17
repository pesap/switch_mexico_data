"""
    Utilities por switch data creation
"""
import os
import sys
import yaml
import glob
import logging
import pandas as pd
from logging.config import fileConfig
from context import *
from IPython.core.debugger import set_trace


logfile_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log.ini')
#  print ( f'Log file configuration located at: {logfile_path}')
fileConfig(logfile_path)
logger = logging.getLogger('Logger')

def look_for_file(filename, path):
    file_path = os.path.join(path, filename)
    match = [pattern for pattern in glob.glob(f'{file_path}.*')]
    if not match:
        return False

    if len(match) < 2:
        return os.path.splitext(match[0])
    elif len(match) > 2:
        click.echo(f'Multiple files detected for {filename}. Please delate one')
        sys.exit(1)
        return False


def read_yaml(path, filename: str):
    """ Read yaml file"""

    file_path = os.path.join(path, filename)

    with open(file_path, 'r') as stream:
        try:
            yaml_file = yaml.load(stream)
        except yaml.YAMLError as exc:
            raise (exc)

    return yaml_file

def create_renewable_data(path: os.PathLike, filename: str):
    logger.debug('often makes a very good meal of %s', 'visiting tourists')
    return (path)

class PowerPlant:
    """
    Power plant class testing
    """

    def __init__(self, uuid, tech, fuel, lz):
        self.uuid = uuid
        self.tech = tech
        self.lz = lz

    def add(self):
        print (type(self))

def create_gen_build_cost_new(df_gen, ext='.tab', path=script_path,
    **kwargs):
    """ Create gen build cost output file

    Args:
        data (pd.DataFrame): dataframe witht dates,
        ext (str): output extension to save the file.

    Note(s):
        * .tab extension is to match the switch inputs,
    """
    cost_table = pd.read_csv('src/gen_cost_reference.csv')
    tech_costs = pd.read_csv('./src/technology_cost.csv')

    if ext == '.tab': sep='\t'

    output_file = output_path + 'gen_build_costs' + ext

    # TODO:  Change the direction of this file

    periods = read_yaml(path, 'periods.yaml')

    # FIXME: This will only work if there is no repeated elements

    gen_costs = pd.merge(df_gen, cost_table, on='gen_tech')
    column_order = ['GENERATION_PROJECT', 'build_year', 'gen_overnight_cost',
            'gen_fixed_om', 'gen_tech']

    output_list = []
    #  output_list.append(gen_costs[cols])
    for period in periods['INVESTMENT_PERIOD']:
        #  print (period)
        gen_costs.loc[:, 'build_year'] = period
        output_list.append(gen_costs[column_order])

    gen_build_cost = pd.concat(output_list)
    gen_build_cost = modify_costs(gen_build_cost)
    gen_build_cost.to_csv('gen_build_cost.tab', sep=sep)

    return (gen_build_cost)

def modify_costs(data, ext='.tab'):
    """ Modify cost data to derate it

    Args:
        data (pd.DataFrame): dataframe witht dates,

    Note(s):
        * This read the cost table and modify the cost by period
    """
    if ext == '.tab': sep='\t'

    output_file = 'gen_build_cost' + ext

    # TODO: Make a more cleaner way to load the file
    cost_table = pd.read_csv('src/cost_tables.csv')

    df = data.copy()

    techo = cost_table['Technology'].unique()
    for index in df.build_year.unique():
        mask = (df['gen_tech'].isin(techo)) & (df['build_year'] == index)
        for tech in df['gen_tech'].unique():
            if tech in cost_table['Technology'].unique():
                mask2 = (cost_table['Technology'] == tech) & (cost_table['Year'] == index)
                cost_table.loc[mask2, 'gen_overnight_cost'].values[0]
                df.loc[mask & (df['gen_tech'] == tech), 'gen_overnight_cost'] = cost_table.loc[mask2, 'gen_overnight_cost'].values[0]
                df.loc[mask & (df['gen_tech'] == tech), 'gen_fixed_om'] = cost_table.loc[mask2, 'gen_fixed_om'].values[0]

    df = df.sort_values(['GENERATION_PROJECT', 'build_year'],
                        ascending=[True, True])

    # TODO: Change direction of the output_file
    # Save file
    df.to_csv(output_file, sep=sep, index=False)

    return (df)

def init_scenario():
    """  Create default scenario with existing technology for each loadzone """

    df_gen = pd.read_csv(os.path.join(default_path,
        'generation_projects_info.tab'), sep='\t')
    column_order = df_gen.columns

    #FIXME: Quick fix to replace tg for turbo_gas
    df_gen['gen_tech'] = df_gen['gen_tech'].replace('tg','turbo_gas')

    gen_default = pd.read_csv(os.path.join(default_path,
                                            'technology_cost.csv'))

    #TODO: This should be a dictionary. Maybe YAML
    load_zones = pd.read_csv('src/load_zones.tab', sep='\t', usecols=[0])

    # Restriction
    restriction = read_yaml(default_path, 'restriction.yaml')

    gen_restriction = {key:
            list(set(df_gen['gen_tech']) - set(df_gen.loc[df_gen['gen_load_zone'] == key, 'gen_tech']))
                        for key in load_zones['LOAD_ZONE']}
    iterator = 1
    prop_gens = []
    for row in load_zones.itertuples():
        lz = row[1]
        gen_lz_restriction = gen_restriction[lz]

        # Get restriction technology by load zone
        lz_restriction = [key for key, value in restriction['technology'].items()
                                                if lz in value]
        tech_rest = set(gen_lz_restriction) | set(lz_restriction)

        # Filter restricted technologiesj
        prop_gen = gen_default.loc[~gen_default['gen_tech'].isin(tech_rest)].copy()

        # Include load zone information
        prop_gen.loc[:, 'gen_load_zone'] = lz

        # Rename generation project
        prop_gen.loc[:, 'GENERATION_PROJECT'] = (prop_gen['GENERATION_PROJECT']
                                                    .map(str.lower)
                                                    + f'_{iterator:03d}')
        prop_gens.append(prop_gen)

        iterator +=1
    df_gen = pd.concat(prop_gens)
    df_gen[column_order].to_csv('generation_test.tab', sep='\t', index=False)

    return df_gen[column_order]


if __name__ == '__main__':
    #  pp = PowerPlant('UUUID', 'solar', 'solar', 'Mulege')
    #  pp.add()
    init_scenario()
    #  gen_build_cost = create_gen_build_cost_new()
    #  modify_costs(gen_build_cost)
