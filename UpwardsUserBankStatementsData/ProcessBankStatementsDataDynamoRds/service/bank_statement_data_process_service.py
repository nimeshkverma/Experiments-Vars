import datetime
from difflib import SequenceMatcher
from database_service import Database
from abc import ABCMeta, abstractmethod

MONTH = {
    1: 'Jan',
    2: 'Feb',
    3: 'Mar',
    4: 'Apr',
    5: 'May',
    6: 'Jun',
    7: 'Jul',
    8: 'Aug',
    9: 'Sep',
    10: 'Oct',
    11: 'Nov',
    12: 'Dec',
}


class ProcessedBankStatementData(object):

    def __init__(self, raw_data):
        self.__raw_data = raw_data
        self.customer_id = self.__get_customer_id()
        self.emi = self.__get_emi()
        self.transactions = self.__get_transactions()
        self.month_wise_data = self.__get_month_wise_data()
        self.insights_sql_query_data = self.__get_insights_sql_query_data()
        self.meta_info_sql_query_data = self.__get_meta_info_sql_query_data()
        self.sql_queries = self.__get_sql_queries()

    def __get_customer_id(self):
        return self.__raw_data['customer_id']['S']

    def __get_emi(self):
        return self.__raw_data['data']['M']['loan_details']['M']['loan_emi']['N']

    def __get_transactions(self):
        transactions = {}
        for day, balance_dict in self.__raw_data['data']['M']['all_transactions']['M'].iteritems():
            transactions[datetime.datetime.strptime(
                day, "%d/%m/%y")] = float(balance_dict['S'])
        return transactions

    def __get_month_wise_data(self):
        get_month_wise_data = {}
        for date, balance in self.transactions.iteritems():
            key = date.year * 100 + date.month
            if not isinstance(get_month_wise_data.get(key), dict):
                get_month_wise_data[key] = {
                    'total_balance': 0,
                    'total_days': 0,
                    'balance_above_emi_days': 0,
                }
            get_month_wise_data[key]['total_balance'] += balance
            get_month_wise_data[key]['total_days'] += 1
            if balance >= self.emi:
                get_month_wise_data[key]['balance_above_emi_days'] += 1
        return get_month_wise_data

    def __get_insights_sql_query_data(self):
        insights_sql_query_data = []
        time_period = self.month_wise_data.keys()
        time_period.sort()
        if time_period and self.month_wise_data[time_period[0]]['total_days'] < 25:
            time_period = time_period[1:]
        if time_period and self.month_wise_data[time_period[-1]]['total_days'] < 25:
            time_period = time_period[:-1]

        for start_index in xrange(len(time_period)):
            cumalative_balance = 0
            cumalative_days = 0
            number_of_days = 0
            month_number = 0
            for index in xrange(start_index, len(time_period)):
                cumalative_balance += self.month_wise_data[
                    time_period[index]]['total_balance']
                cumalative_days += self.month_wise_data[
                    time_period[index]]['balance_above_emi_days']
                number_of_days += 30
                year = time_period[index] / 100
                month = time_period[index] % 100
                month_number += 1
                balance_data_dict = {
                    'month': MONTH[month],
                    'year': year,
                    'days_in_period': number_of_days,
                    'attribute_name': 'moving_average_balance',
                    'attribute_type': 'balance',
                    'attribute_value': int(cumalative_balance / number_of_days)
                }
                insights_sql_query_data.append(balance_data_dict)
                emi_above_days_data_dict = {
                    'month': MONTH[month],
                    'year': year,
                    'days_in_period': number_of_days,
                    'attribute_name': 'moving_average_balance',
                    'attribute_type': 'count',
                    'attribute_value': int(cumalative_days / month_number)
                }
                insights_sql_query_data.append(emi_above_days_data_dict)
        for time_period_key, time_period_value in self.month_wise_data.iteritems():
            analysed_days_data_dict = {
                'month': time_period[index] % 100,
                'year': time_period[index] / 100,
                'days_in_period': time_period_value['total_days'],
                'attribute_name': 'days_analysed',
                'attribute_type': 'count',
                'attribute_value': time_period_value['total_days'],
            }
            insights_sql_query_data.append(analysed_days_data_dict)
        return insights_sql_query_data

    def __get_insights_sql_query(self):
        sql_query = """ INSERT INTO analytics_bank_statement_insights
                        (customer_id, attribute_name, attribute_type, attribute_value,
                         month, year, days_in_period,  created_at, updated_at, is_active)
                        VALUES 
                    """
        for data in self.insights_sql_query_data:
            sql_query += """({customer_id}, '{attribute_name}', '{attribute_type}', {attribute_value}, '{month}', {year}, {days_in_period}, ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), TRUE) ,""".format(
                customer_id=self.customer_id,
                attribute_name=data['attribute_name'],
                attribute_value=data['attribute_value'],
                attribute_type=data['attribute_type'],
                month=data['month'],
                year=data['year'],
                days_in_period=data['days_in_period'],
            )
        sql_query = sql_query[:-1]
        # sql_query += """ON CONFLICT (customer_id)
        #                 DO UPDATE SET
        #                 created_at = excluded.created_at,
        #                 updated_at = excluded.updated_at,
        #                 is_active = excluded.is_active;"""
        sql_query += " ; "
        return sql_query

    def __get_meta_info_sql_query_data(self):
        meta_info_sql_query_data = {}
        for stats_key, stats_value_dict in self.__raw_data['data']['M']['stats']['M'].iteritems():
            meta_info_sql_query_data[stats_key] = stats_value_dict.values()[0]

        meta_info_sql_query_data['bank_name'] = self.__raw_data[
            'data']['M']['bank_name']['S']
        return meta_info_sql_query_data

    def __get_meta_info_sql_query(self):
        sql_query = """ INSERT INTO analytics_bank_statement_meta_info
                        (customer_id, attribute_name, attribute_value,
                         created_at, updated_at, is_active)
                        VALUES 
                    """
        for attribute_name, attribute_value in self.meta_info_sql_query_data.iteritems():
            sql_query += """({customer_id}, '{attribute_name}', '{attribute_value}', ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), ( select now()::timestamp with time zone at time zone 'Asia/Kolkata'), TRUE) ,""".format(
                customer_id=self.customer_id,
                attribute_name=attribute_name,
                attribute_value=attribute_value,
            )
        sql_query = sql_query[:-1]
        # sql_query += """ON CONFLICT (customer_id)
        #                 DO UPDATE SET
        #                 created_at = excluded.created_at,
        #                 updated_at = excluded.updated_at,
        #                 is_active = excluded.is_active;"""
        sql_query += " ; "
        return sql_query

    def __get_sql_queries(self):
        sql_queries = [
            self.__get_insights_sql_query(),
            self.__get_meta_info_sql_query(),
        ]
        return sql_queries
