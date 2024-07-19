from django.db import connection

from mam_api_app.utils.sql_util import SqlUtil
from mam_api_app.models.forecast_result import ForecastResult


class ForecastRepo:

    @staticmethod
    def check_pax_exist(airport, year, month):
        sql_query = """
            SELECT EXISTS (SElECT 1 FROM airport_monthly_pax WHERE airport = %s AND year = %s AND month = %s AND approved LIMIT 1);
        """
        cursor = connection.cursor()
        cursor.execute(sql_query, [airport, year, month])
        rqs = SqlUtil.dictfetchall(cursor)
        return rqs[0]

    @staticmethod
    def get_last_2_forecast_by_atf(airport):
        sql_query = """
                    SELECT 
                        1 as id, date, pax, airport, terminal, flight_type, direction, forecast_type
                    FROM 
                        public.last_2_forecast_by_atf
                    WHERE
                        airport = %s
                    AND
                        date > (SELECT MAX(date) - interval '2 years' FROM public.last_2_forecast_by_atf WHERE airport = %s)
                    """

        rqs = ForecastResult.objects.raw(sql_query, [airport, airport])
        return rqs

    @staticmethod
    def get_actual_data_by_airport(airport):
        sql_query = """
                    SELECT 
                        year, month, terminal, flight_type, sum(pax) as actual_data
                    FROM 
                        public.airport_monthly_pax
                    WHERE
                        airport = %s
                        AND
                        year >= (SELECT MAX(year) FROM public.airport_monthly_pax WHERE airport = %s)
                        AND
                        approved
                    GROUP BY year, month, terminal, flight_type 
                    ORDER BY year, month, terminal, flight_type;
        """
        cursor = connection.cursor()
        cursor.execute(sql_query, [airport, airport])
        rqs = SqlUtil.dictfetchall(cursor)
        return rqs

    @staticmethod
    def get_forwardkeys_year_month(airport):
        sql_query = '''
            SELECT
                DISTINCT yearmonth
            FROM
                public.forwardkeys_daily
            WHERE
                airport = %s
            ORDER BY
                yearmonth ASC
        '''
        cursor = connection.cursor()
        cursor.execute(sql_query, [airport])
        rqs = SqlUtil.dictfetchall(cursor)
        return rqs

    @staticmethod
    def get_airport_year_month(airport):
        sql_query = '''
            SELECT
                DISTINCT year, month
            FROM
                public.airport_monthly_pax
            WHERE
                airport = %s
                AND
                approved
            ORDER BY
                year ASC, month ASC
        '''
        cursor = connection.cursor()
        cursor.execute(sql_query, [airport])
        rqs = SqlUtil.dictfetchall(cursor)
        return rqs

    @staticmethod
    def approve_airport_pax(airport, year, month):
        sql_query = '''
            UPDATE
                public.airport_monthly_pax
            SET
                approved = true
            WHERE
                airport = %s AND year = %s AND month = %s
        '''
        cursor = connection.cursor()
        cursor.execute(sql_query, [airport, year, month])



    @staticmethod
    def get_all_history_pax_by_airport(airport):
        sql_query = """
                    SELECT 
                        year, month, airport, terminal, direction, flight_type, pax
                    FROM 
                        public.airport_monthly_pax
                    WHERE
                        airport = %s
                        AND
                        approved
        """
        cursor = connection.cursor()
        cursor.execute(sql_query, [airport])
        rqs = SqlUtil.dictfetchall(cursor)
        return rqs



