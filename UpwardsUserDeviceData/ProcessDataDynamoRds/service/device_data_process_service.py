import datetime
from database_service import Database
from abc import ABCMeta, abstractmethod


class ProcessedDeviceData(object):

    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.customer_id = self.get_customer_id()
        self.status_mapping = {
            'INCOMING': 'Incoming',
            'OUTGOING': 'Outgoing',
        }

    def get_customer_id(self):
        return self.raw_data['customer_id']['S']

    def get_day_type(self, milli_seconds):
        day_of_the_week = datetime.datetime.fromtimestamp(
            int(milli_seconds) / 1000).weekday()
        if day_of_the_week in [5, 6]:
            return 'Weekend'
        else:
            return 'Weekday'

    def get_hour_type(self, milli_seconds):
        hour_of_the_day = datetime.datetime.fromtimestamp(
            int(milli_seconds) / 1000).hour
        if hour_of_the_day >= 6 and hour_of_the_day < 10:
            return 'Morning'
        elif hour_of_the_day >= 10 and hour_of_the_day < 19:
            return 'Office Hours'
        elif hour_of_the_day >= 19 and hour_of_the_day < 22:
            return 'Evening'
        else:
            return 'Late Night'

    def process_raw_data(self):
        pass

    def process_aggregate_data(self):
        pass

    def get_sql_query(self):
        sql_query = """
                INSERT INTO analytics_devicedata 
                (customer_id, data_type, status, attribute, value, weekday_type, day_hour_type, created_at, updated_at, is_active) 
                    VALUES 
                    """
        for attribute in self.attributes:
            for status, device_data in self.aggregate_data[attribute].iteritems():
                for day_type, day_device_data in device_data.iteritems():
                    for day_hour_type, value in day_device_data.iteritems():
                        sql_query += """({customer_id}, '{data_type}', '{status}', '{attribute}', {value}, '{weekday_type}', '{day_hour_type}', ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), TRUE) ,""".format(
                            customer_id=self.customer_id,
                            data_type=self.data_type,
                            status=status,
                            attribute=attribute,
                            value=value,
                            weekday_type=day_type,
                            day_hour_type=day_hour_type,
                        )
        for attribute in self.ratio_types:
            for day_type, day_device_data in self.aggregate_data[attribute].iteritems():
                for day_hour_type, value in day_device_data.iteritems():
                    sql_query += """({customer_id}, '{data_type}', '{status}', '{attribute}', {value}, '{weekday_type}', '{day_hour_type}', ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), TRUE)
                                ,""".format(
                        customer_id=self.customer_id,
                        data_type=self.data_type,
                        status='Outgoing/Incoming',
                        attribute=attribute,
                        value=value,
                        weekday_type=day_type,
                        day_hour_type=day_hour_type,
                    )
        sql_query = sql_query[:-1]
        sql_query += """ ON CONFLICT (customer_id, data_type, status, attribute, weekday_type, day_hour_type) 
                        DO UPDATE SET 
                        created_at = excluded.created_at, 
                        updated_at = excluded.updated_at,
                        is_active = excluded.is_active,
                        value = excluded.value;"""
        return sql_query


class ProcessedCallData(ProcessedDeviceData):

    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.customer_id = self.get_customer_id()
        self.status_mapping = {
            'INCOMING': 'Incoming',
            'OUTGOING': 'Outgoing',
        }
        self.aggregate_data = {
            'Count': {
                'Incoming': {
                    'Weekday': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Weekend': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Week': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                },
                'Outgoing': {
                    'Weekday': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Weekend': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Week': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                },
            },
            'Duration': {
                'Incoming': {
                    'Weekday': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Weekend': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Week': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                },
                'Outgoing': {
                    'Weekday': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Weekend': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Week': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                },
            },
            'Count Ratio': {
                'Weekday': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
                'Weekend': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
                'Week': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
            },
            'Duration Ratio': {
                'Weekday': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
                'Weekend': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
                'Week': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
            },
        }
        self.ratio_types = ['Count Ratio', 'Duration Ratio']
        self.attributes = ['Count', 'Duration']
        self.data_type = 'Call'
        self.process_raw_data()
        self.process_aggregate_data()
        self.sql_query = self.get_sql_query()

    def process_raw_data(self):
        for call_data in self.raw_data.get('data', {}).get('M', {}).get('data', {}).get('L', []):
            call_status = call_data.get('M', {}).get('Call Type', {}).get('S')
            if call_status in self.status_mapping.keys():
                call_date = call_data['M']['Call Date']['S']
                day_type = self.get_day_type(call_date)
                hour_type = self.get_hour_type(call_date)
                call_duration = int(call_data['M']['Call Duration']['S'])
                self.aggregate_data['Count'][
                    self.status_mapping[call_status]][day_type]['All'] += 1
                self.aggregate_data['Count'][self.status_mapping[
                    call_status]][day_type][hour_type] += 1
                self.aggregate_data['Count'][self.status_mapping[
                    call_status]]['Week'][hour_type] += 1
                self.aggregate_data['Duration'][
                    self.status_mapping[call_status]][day_type]['All'] += call_duration
                self.aggregate_data['Duration'][self.status_mapping[
                    call_status]][day_type][hour_type] += call_duration
                self.aggregate_data['Duration'][self.status_mapping[
                    call_status]]['Week'][hour_type] += call_duration

    def process_aggregate_data(self):
        for ratio_type in self.ratio_types:
            type_key = 'Count' if ratio_type == 'Count Ratio' else 'Duration'
            for day_type_key, day_type_value in self.aggregate_data[ratio_type].iteritems():
                for hour_type_key in day_type_value.keys():
                    incoming = self.aggregate_data[type_key][
                        'Incoming'][day_type_key][hour_type_key]
                    outgoing = self.aggregate_data[type_key][
                        'Outgoing'][day_type_key][hour_type_key]
                    self.aggregate_data[ratio_type][day_type_key][hour_type_key] = round(
                        outgoing * 100.0 / incoming, 4) if incoming else 9999


class ProcessedSMSData(ProcessedDeviceData):

    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.customer_id = self.get_customer_id()
        self.status_mapping = {
            '1': 'Incoming',
            '2': 'Outgoing',
        }
        self.aggregate_data = {
            'Count': {
                'Incoming': {
                    'Weekday': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Weekend': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Week': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                },
                'Outgoing': {
                    'Weekday': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Weekend': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                    'Week': {
                        'Morning': 0.0,
                        'Office Hours': 0.0,
                        'Evening': 0.0,
                        'Late Night': 0.0,
                        'All': 0.0
                    },
                },
            },
            'Count Ratio': {
                'Weekday': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
                'Weekend': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
                'Week': {
                    'Morning': 0.0,
                    'Office Hours': 0.0,
                    'Evening': 0.0,
                    'Late Night': 0.0,
                    'All': 0.0
                },
            },
        }
        self.ratio_types = ['Count Ratio']
        self.attributes = ['Count']
        self.data_type = 'SMS'
        self.process_raw_data()
        self.process_aggregate_data()
        self.sql_query = self.get_sql_query()

    def process_raw_data(self):
        for sms_data in self.raw_data.get('data', {}).get('M', {}).get('data', {}).get('L', []):
            sms_status = sms_data.get('M', {}).get('Type', {}).get('S')
            if sms_status in self.status_mapping.keys():
                sms_date = sms_data['M']['Date']['S']
                day_type = self.get_day_type(sms_date)
                hour_type = self.get_hour_type(sms_date)
                self.aggregate_data['Count'][
                    self.status_mapping[sms_status]][day_type]['All'] += 1
                self.aggregate_data['Count'][
                    self.status_mapping[sms_status]][day_type][hour_type] += 1
                self.aggregate_data['Count'][
                    self.status_mapping[sms_status]]['Week'][hour_type] += 1

    def process_aggregate_data(self):
        for ratio_type in self.ratio_types:
            type_key = 'Count'
            for day_type_key, day_type_value in self.aggregate_data[ratio_type].iteritems():
                for hour_type_key in day_type_value.keys():
                    incoming = self.aggregate_data[type_key][
                        'Incoming'][day_type_key][hour_type_key]
                    outgoing = self.aggregate_data[type_key][
                        'Outgoing'][day_type_key][hour_type_key]
                    self.aggregate_data[ratio_type][day_type_key][hour_type_key] = round(
                        outgoing * 100.0 / incoming, 4) if incoming else 9999


class ProcessedContactData(object):

    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.customer_id = self.get_customer_id()
        self.aggregate_data = {
            'Number of Contacts': 0
        }
        self.process_raw_data()
        self.sql_query = self.get_sql_query()

    def get_customer_id(self):
        return self.raw_data['customer_id']['S']

    def process_raw_data(self):
        contact_data = self.raw_data.get('data', {}).get(
            'M', {}).get('data', {}).get('L', [])
        self.aggregate_data['Number of Contacts'] += len(contact_data)

    def get_sql_query(self):
        sql_query = """
                INSERT INTO analytics_contactdata 
                (customer_id, data_type, value, created_at, updated_at, is_active) 
                    VALUES 
                    """
        for data_type, value in self.aggregate_data.iteritems():
            sql_query += """({customer_id}, '{data_type}', {value}, ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), TRUE) ,""".format(
                customer_id=self.customer_id,
                data_type=data_type,
                value=value,
            )
        sql_query = sql_query[:-1] + ' ;'
        return sql_query
