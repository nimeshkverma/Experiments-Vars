import json
import logging
from service.database_service import Database
from service.event_data_process_service import ProcessedEventsData
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Loading function')


def lambda_handler(event, context):
    db = Database()
    logger.info("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        if record.get('dynamodb', {}).get('NewImage') and record.get('eventName') == 'INSERT':
            sql_queries = ProcessedEventsData(
                record['dynamodb']['NewImage']).sql_queries
            for sql_query in sql_queries:
                db.execute_query(sql_query)
            logger.info('Event Data Processed')
        else:
            logger.info('Event Data record not found for Dynamodb')
    db.close_connection()
    return 'Successfully processed {} events records.'.format(len(event['Records']))
