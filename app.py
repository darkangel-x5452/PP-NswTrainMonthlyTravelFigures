from flask import Flask, g, jsonify, make_response
from flask_restplus import Api, Resource, fields
import sqlite3
from os import path

app = Flask(__name__)
api = Api(app, version='1.0', title='Data Service for NSW train line monthly opal trips. July 2016 to April 2019.',
          description='This is a Flask-Restplus data service that allows a client to consume APIs related to NSW train line monthly opal trips from July 2016 to April 2019.',
          )

# Database helper
ROOT = path.dirname(path.realpath(__file__))


def connect_db():
    sql = sqlite3.connect(path.join(ROOT, "NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019.sqlite"))
    sql.row_factory = sqlite3.Row
    return sql


def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@api.route('/all')
class NSWOpalAll(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for all train lines.')
    def get(self):
        db = get_db()
        details_cur = db.execute('select TRAIN_LINE, PERIOD, COUNT from NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019')
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/<string:PERIOD>', methods=['GET'])
class NSWOpalPeriod(Resource):
    print('<------------ PERIOD i am here ------------>')

    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database selected period.')
    def get(self, PERIOD):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, PERIOD, COUNT from NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019 where PERIOD = ? COLLATE NOCASE',
            [PERIOD])
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/<string:TRAIN_LINE>', methods=['GET'])
class NSWOpalTrainLine(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for selected train line.')
    def get(self, TRAIN_LINE):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, PERIOD, COUNT from NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019 where TRAIN_LINE like ? COLLATE NOCASE',
            ["%" + TRAIN_LINE + "%"])
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/<string:TRAIN_LINE>/<string:PERIOD>', methods=['GET'])
class NSWOpalTrainLinePeriod(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for selected train line and period.')
    def get(self, TRAIN_LINE, PERIOD):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, PERIOD, COUNT from NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019 where (TRAIN_LINE like ? and PERIOD = ?) COLLATE NOCASE',
            ["%" + TRAIN_LINE + "%", PERIOD])
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


if __name__ == '__main__':
    app.run()
