import json
import logging
from service.database_service import Database
from service.device_data_process_service import ProcessedCallData, ProcessedSMSData, ProcessedContactData
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Loading function')


def get_data_class(record):
    if 'UpwardsUserCallData' in record.get('eventSourceARN'):
        return ProcessedCallData
    elif 'UpwardsUserSMSData' in record.get('eventSourceARN'):
        return ProcessedSMSData
    elif 'UpwardsUserContactData' in record.get('eventSourceARN'):
        return ProcessedContactData
    else:
        return None


def lambda_handler(event, context):
    db = Database()
    logger.info("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        if record.get('dynamodb', {}).get('NewImage') and record.get('eventName') == 'INSERT':
            data_class = get_data_class(record)
            if data_class:
                sql_query = data_class(
                    record['dynamodb']['NewImage']).sql_query
                db.execute_query(sql_query)
                logger.info('Data Processed')
            else:
                logger.info('Data record not found for SMS or Call')
        else:
            logger.info('Data record not found for Dynamodb')
    db.close_connection()
    return 'Successfully processed {} records.'.format(len(event['Records']))
