import logging
from mds_data_prep import mds_data_prep

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# https://docs.aws.amazon.com/lambda/latest/dg/python-package.html

def lambda_handler(event, context):
    insert_df = mds_data_prep(local=True)

lambda_handler(None, None)