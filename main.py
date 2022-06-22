from flask import Flask
import os
import time
import datetime
from dotenv import load_dotenv
from database import Database
from database import CursorFromConnectionFromPool
from flask_cors import CORS, cross_origin


load_dotenv()
DB_PWD = os.getenv('DB_PWD')
DB_USER = os.getenv('DB_USER')
DB_HOST = os.getenv('DB_HOST')

DB = os.getenv('DB')
app = Flask(__name__)
CORS(app)
Database.initialize(user=f'{DB_USER}',
                    password=f'{DB_PWD}',
                    host=f'{DB_HOST}',
                    port=5432,
                    database=f'{DB}')


@app.route('/events/hourly')
@cross_origin()
def events_hourly():
    time.sleep(1)
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute('SELECT h.date, h.hour, h.events, poi.name FROM hourly_events as h join poi on '
                       'h.poi_id=poi.poi_id ORDER BY date, hour LIMIT 168;')
        results = cursor.fetchall()

    result_list = []
    date_list = {}
    for result in results:
        date = result[0].strftime("%Y-%m-%d")
        new_result = {'date': date, 'hour': result[1], 'events': result[2], 'poiName': result[3]}
        result_list.append(new_result)

        short_result = {'hour': result[1], 'events': result[2], 'poiName': result[3]}
        if date not in date_list:
            new_element = {
                date: []
            }
            date_list.update(new_element)
            date_list[date].append(short_result)
        else:
            date_list[date].append(short_result)
    return{'hourlyResult': result_list, 'eventForEachDay': date_list}


@app.route('/events/daily')
@cross_origin()
def events_daily():
    time.sleep(1)
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute('SELECT date, SUM(events) AS events FROM public.hourly_events '
                       'GROUP BY date ORDER BY date LIMIT 7;')
        results = cursor.fetchall()

    with CursorFromConnectionFromPool() as cursor:
        cursor.execute('SELECT h.date, sum(h.events), poi.name from hourly_events as h join poi on h.poi_id=poi.poi_id '
                       'group by h.date, poi.name ORDER BY date LIMIT 7;')
        results_geo = cursor.fetchall()

    result_list = []
    geo_result_list = []
    date_list={}
    for result in results:
        date = result[0].strftime("%Y-%m-%d")
        new_result = {'date': date, 'eventSum': result[1]}
        result_list.append(new_result)

    for result_geo in results_geo:
        date = result_geo[0].strftime("%Y-%m-%d")
        new_geo_result = {'date': date, 'eventSum': result_geo[1], 'poiName': result_geo[2]}
        geo_result_list.append(new_geo_result)

        short_result = {'events': result_geo[1], 'poiName': result_geo[2]}
        if date not in date_list:
            new_element = {
                date: []
            }
            date_list.update(new_element)
            date_list[date].append(short_result)
        else:
            date_list[date].append(short_result)
    return {'dailyResult': result_list, 'resultsForGeo': geo_result_list,'dailyResultStat': date_list}


@app.route('/stats/hourly')
@cross_origin()
def stats_hourly():
    time.sleep(1)
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute('SELECT h.date, h.hour, h.impressions, h.clicks, h.revenue, poi.name from hourly_stats as h '
                       'join poi on h.poi_id=poi.poi_id ORDER BY date, hour LIMIT 168;')
        results = cursor.fetchall()

    result_list = []
    date_list = {}
    for result in results:
        date = result[0].strftime("%Y-%m-%d")
        new_result = {'date': date, 'hour': result[1], 'impressions': result[2], 'clicks': result[3],
                      'revenue': round(result[4],2), 'poiName': result[5]}


        short_result = {'hour': result[1], 'impressions': result[2], 'clicks': result[3],
                        'revenue': result[4], 'poiName': result[5]}
        result_list.append(new_result)
        if date not in date_list:
            new_element = {
                date: []
            }
            date_list.update(new_element)
            date_list[date].append(short_result)
        else:
            date_list[date].append(short_result)
    return {'hourlyStatResult': result_list, 'statForEachDay': date_list}


@app.route('/stats/daily')
@cross_origin()
def stats_daily():
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute('SELECT date, SUM(impressions) AS impressions, '
                       'SUM(clicks) AS clicks, SUM(revenue) AS revenue '
                       'FROM public.hourly_stats GROUP BY date ORDER BY date LIMIT 7;')
        results = cursor.fetchall()

    with CursorFromConnectionFromPool() as cursor:
        cursor.execute('SELECT h.date, SUM(h.impressions) AS impressions, SUM(h.clicks) AS clicks, SUM(h.revenue)'
                       ' AS revenue, poi.name FROM hourly_stats as h join poi on h.poi_id=poi.poi_id GROUP BY h.date,'
                       ' poi.name ORDER BY date LIMIT 7;')
        geo_results = cursor.fetchall()

    result_list = []
    geo_result_list = []
    date_list={}
    for result in results:
        date = result[0].strftime("%Y-%m-%d")
        new_result = {'date': date, 'sumImpressions': result[1], 'sumClicks': result[2], 'sumRevenue': result[3]}
        result_list.append(new_result)

    for result in geo_results:
        date = result[0].strftime("%Y-%m-%d")
        new_geo_result = {'date': date, 'sumImpressions': result[1], 'sumClicks': result[2], 'sumRevenue': round(result[3],2),
                          'poiName': result[4]}
        geo_result_list.append(new_geo_result)

        short_result = { 'sumImpressions': result[1], 'sumClicks': result[2],
                        'sumRevenue': round(result[3],2), 'poiName': result[4]}
        if date not in date_list:
            new_element = {
                date: []
            }
            date_list.update(new_element)
            date_list[date].append(short_result)
        else:
            date_list[date].append(short_result)
    return {'dailyStatResult': result_list, 'statForGeo': geo_result_list, 'dailyResultStat': date_list}


@app.route('/poi')
def poi():
    time.sleep(1)
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute('SELECT * FROM public.poi;')
        results = cursor.fetchall()

    result_list = []
    geo_result={}
    for result in results:
        new_result = {'poi_id': result[0], 'name': result[1], 'lat': result[2], 'lon': result[3]}
        result_list.append(new_result)

        short_result={'lat': result[2], 'lon': result[3]}
        new_geo_result={result[1]:[]}
        geo_result.update(new_geo_result)
        geo_result[result[1]].append(short_result)
    return {'poiResult': result_list, 'geoPoiResult': geo_result}


if __name__ == "__main__":
    app.run(debug=True)
