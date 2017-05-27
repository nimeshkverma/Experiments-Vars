import datetime
from database_service import Database
from abc import ABCMeta, abstractmethod


class ProcessedEventsData(object):

    def __init__(self, raw_data):
        self.__raw_data = raw_data
        self.customer_id = self.__get_customer_id()
        self.__screen_data = {
            'aadhaar_details': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'aadhaar': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'bank': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'documents': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'education': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'eligibility_review': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'finance': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'kyc_review': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'loan_product': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'pan': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'personal_contact': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'profession': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            },
            'signup': {
                'create': {
                    'sessions': 0,
                    'time_spent': 0
                },
                'update': {
                    'sessions': 0,
                    'time_spent': 0
                }
            }
        }
        self.__field_data = {
            'aadhaar_details': {
                'create': {},
                'update': {}
            },
            'aadhaar': {
                'create': {},
                'update': {}
            },
            'bank': {
                'create': {},
                'update': {}
            },
            'documents': {
                'create': {},
                'update': {}
            },
            'education': {
                'create': {},
                'update': {}
            },
            'eligibility_review': {
                'create': {},
                'update': {}
            },
            'finance': {
                'create': {},
                'update': {}
            },
            'kyc_review': {
                'create': {},
                'update': {}
            },
            'loan_product': {
                'create': {},
                'update': {}
            },
            'pan': {
                'create': {},
                'update': {}
            },
            'personal_contact': {
                'create': {},
                'update': {}
            },
            'profession': {
                'create': {},
                'update': {}
            },
            'signup': {
                'create': {},
                'update': {}
            }
        }
        self.__process_raw_data()
        self.sql_queries = self.__get_sql_queries()

    def __get_customer_id(self):
        return self.__raw_data['customer_id']['S']

    def __get_mode(self, event):
        return event['M']['data']['M']['field_events']['L'][0]['M']['data']['M']['mode']['S']

    def __process_raw_data(self):
        for screen_event_data in self.__raw_data['data']['L']:
            screen = screen_event_data['M']['data']['M']['from_screen']['S']
            mode = self.__get_mode(screen_event_data)
            for session_timestamp in screen_event_data['M']['data']['M']['session_timestamp']['L']:
                self.__screen_data[screen][mode]['sessions'] += 1
                self.__screen_data[screen][mode]['time_spent'] += int(session_timestamp['M'][
                    'end_time']['N']) - int(session_timestamp['M']['start_time']['N'])
            for field_event in screen_event_data['M']['data']['M']['field_events']['L']:
                field_name = field_event['M']['name']['S']
                if field_name in self.__field_data[screen][mode]:
                    self.__field_data[screen][mode][field_name] += 1
                else:
                    self.__field_data[screen][mode][field_name] = 1

    def __get_screen_sql_query(self):
        sql_query = """ INSERT INTO analytics_screen_eventdata 
                        (customer_id, time_spent, sessions, screen, mode, created_at, updated_at, is_active) 
                        VALUES 
                    """
        for screen, data in self.__screen_data.iteritems():
            for mode, mode_data in data.iteritems():
                sql_query += """({customer_id}, {time_spent}, {sessions}, '{screen}', '{mode}', ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), TRUE) ,""".format(
                    customer_id=self.customer_id,
                    time_spent=mode_data['time_spent'],
                    sessions=mode_data['sessions'],
                    screen=screen,
                    mode=mode,
                )
        sql_query = sql_query[:-1] + ' ;'
        return sql_query

    def __get_field_sql_query(self):
        sql_query = """ INSERT INTO analytics_field_eventdata 
                        (customer_id, field, edits, screen, mode, created_at, updated_at, is_active) 
                        VALUES 
                    """
        for screen, data in self.__field_data.iteritems():
            for mode, mode_data in data.iteritems():
                for field, edits in mode_data.iteritems():
                    sql_query += """({customer_id}, '{field}', {edits}, '{screen}', '{mode}', ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), TRUE) ,""".format(
                        customer_id=self.customer_id,
                        field=field,
                        edits=edits,
                        screen=screen,
                        mode=mode,
                    )
        sql_query = sql_query[:-1] + ' ;'
        return sql_query

    def __get_sql_queries(self):
        return [self.__get_field_sql_query(),
                self.__get_screen_sql_query()]
