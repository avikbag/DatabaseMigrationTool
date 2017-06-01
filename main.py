from databaseImport import sqliteRead as sr
import logging 
import os 
import logging.config
import yaml

def main():
    name = '../150309_m610_m405_s1.LEVER'
    logger.info('Imported file {0}'.format(name))
    sql = sr(name)
    sql.getSchema()
    sql.dropTables()
    sql.createTables()
    sql.populate()
    logger.debug('Query Complete')

def setup_logging(
    default_path='logging.yaml',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)
    main()
