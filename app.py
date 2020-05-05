# Paolo Vega
# SQLAlchemy Challenge
# Bootcamp
# Versión 1.0.0 May-03-2020
# Versión 1.0.1 May-03-2020
# Versión 1.0.2 May-03-2020 (updates and comments)
# Versión 1.0.3 May-04-2020

#################################################
# Import Modules
#################################################

from flask import Flask, jsonify
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime as dt
from datetime import timedelta
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import matplotlib.ticker as ticker
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

#################################################
# DB Connection
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################


@app.route("/")
def welcome():
    return '''
        <html>
            <head>
                <title>Precipitation</title>
            </head>
            <body>
                <h1>Welcome to the Weather Vacation Analysis!</h1>
                <h2>Available Routes:</h2>
                <hr />
                <h3>/api/v1.0/precipitation</h3>
                <h3>/api/v1.0/stations</h3>
                <h3>/api/v1.0/tobs</h3>
                <h3>/api/v1.0/50/80</h3>
            </body>
        </html>
        '''
    
@app.route("/api/v1.0/precipitation")
def precipitation():
    try:
        engine = create_engine("sqlite:///Resources/hawaii.sqlite")
        # reflect an existing database into a new model
        Base = automap_base()
        # reflect the tables
        Base.prepare(engine, reflect=True)
        # Save references to each table
        Station = Base.classes.station
        Measurement = Base.classes.measurement
        # Create our session (link) from Python to the DB
        session = Session(engine)
        # get the last date in the DB
        max_date = session.query(func.max(Measurement.date).label('last_date')).all()[0][0]
        # Convert to date
        max_date = dt.datetime.strptime(max_date,"%Y-%m-%d")
        # Calculate  the date - one year ago
        one_year_ago = max_date - dt.timedelta(days=365)
        sel = [Measurement.date,func.sum(Measurement.prcp)]
        precipitation = session.query(*sel).\
            filter(Measurement.date >= func.strftime("%Y-%m-%d",one_year_ago)).\
            filter(Measurement.date <= func.strftime("%Y-%m-%d",max_date)).\
            group_by(Measurement.date).\
            order_by(Measurement.date).all()
        # Transform into dataframe
        precipitation = pd.DataFrame(precipitation, columns=["day","prcp"])
        # update datatype for day
        precipitation["day"] = pd.to_datetime(precipitation["day"])
        # update index to handle plot easily
        precipitation.set_index("day",inplace=True)
        return jsonify(precipitation.to_json())
    except:
        return jsonify({"error":" There is an error with the query. Try agan later"}), 404

@app.route("/api/v1.0/stations")
def stations():
    try:
        engine = create_engine("sqlite:///Resources/hawaii.sqlite")
        # reflect an existing database into a new model
        Base = automap_base()
        # reflect the tables
        Base.prepare(engine, reflect=True)
        # Save references to each table
        Station = Base.classes.station
        Measurement = Base.classes.measurement
        # Create our session (link) from Python to the DB
        session = Session(engine)
        sel_act_stations = [Station.name,func.count(Measurement.date)]
        active_stations = session.query(*sel_act_stations).filter(Station.station == Measurement.station).group_by(Station.station).order_by(func.count(Station.station).desc()).all()
        active_stations_df = pd.DataFrame(active_stations, columns=["station","rows"])
        active_stations_df.set_index("station",inplace=True)
        active_stations_df["rows"] = active_stations_df['rows'].map('{:,}'.format)
        return jsonify(active_stations_df.to_json())
    except:
        return jsonify({"error":" There is an error with the query. Try agan later"}), 404


@app.route("/api/v1.0/tobs")
def temperature():
    try:
        engine = create_engine("sqlite:///Resources/hawaii.sqlite")
        # reflect an existing database into a new model
        Base = automap_base()
        # reflect the tables
        Base.prepare(engine, reflect=True)
        # Save references to each table
        Station = Base.classes.station
        Measurement = Base.classes.measurement
        # Create our session (link) from Python to the DB
        session = Session(engine)
        last_date = session.query(func.max(Measurement.date).label('last_date')).subquery('t')
        sel_waihee_data = [Measurement.date, Measurement.tobs]
        waihee_data = session.query(*sel_waihee_data).\
            filter(Measurement.station == 'USC00519281').\
            filter(Measurement.date >= func.strftime("%Y-%m-%d",one_year_ago)).\
            filter(Measurement.date <= func.strftime("%Y-%m-%d",max_date)).\
            group_by(Measurement.date).\
            order_by(Measurement.date).all()
        # Transform into dataframe
        waihee_df = pd.DataFrame(waihee_data, columns=["day","temp"])
        # update datatype for day
        waihee_df["day"] = pd.to_datetime(waihee_df["day"])
        # update index to handle plot easily
        waihee_df.set_index("day",inplace=True)
        # See sample data
        return jsonify(waihee_df.to_json())
    except:
        return jsonify({"error":" There is an error with the query. Try agan later"}), 404



@app.route("/api/v1.0/<start>")
def start_temperature(start):
    print(f"parameter: {start} - {type(start)}")
    engine = create_engine("sqlite:///Resources/hawaii.sqlite")
    # reflect an existing database into a new model
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    # Save references to each table
    Station = Base.classes.station
    Measurement = Base.classes.measurement
    # Create our session (link) from Python to the DB
    session = Session(engine)
    sel_list_temp = [Measurement.date, func.min(Measurement.tobs).label('min_temp'), func.avg(Measurement.tobs).label('avg_temp'), func.max(Measurement.tobs).label('max_temp') ]
    temp = session.query(*sel_list_temp).filter(Measurement.date >= start).group_by(Measurement.date).order_by(Measurement.date).all()
    # Transform into dataframe
    temp_df = pd.DataFrame(temp, columns=["day","Min Temp","Avg Temp","Max Temp"])
    # update datatype for day
    temp_df["day"] = pd.to_datetime(temp_df["day"])
    # update index to handle plot easily
    temp_df.set_index("day",inplace=True)
    # See sample data
    return jsonify(temp_df.to_json())


@app.route("/api/v1.0/<start>/<end>")
def range_temperature(start,end):
    engine = create_engine("sqlite:///Resources/hawaii.sqlite")
    # reflect an existing database into a new model
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    # Save references to each table
    Station = Base.classes.station
    Measurement = Base.classes.measurement
    # Create our session (link) from Python to the DB
    session = Session(engine)
    sel_list_temp = [Measurement.date, func.min(Measurement.tobs).label('min_temp'), func.avg(Measurement.tobs).label('avg_temp'), func.max(Measurement.tobs).label('max_temp') ]
    temp = session.query(*sel_list_temp).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).\
        group_by(Measurement.date).\
        order_by(Measurement.date).all()
    # Transform into dataframe
    temp_df = pd.DataFrame(temp, columns=["day","Min Temp","Avg Temp","Max Temp"])
    # update datatype for day
    temp_df["day"] = pd.to_datetime(temp_df["day"])
    # update index to handle plot easily
    temp_df.set_index("day",inplace=True)
    # See sample data
    return jsonify(temp_df.to_json())



if __name__ == "__main__":
    app.run(debug=True)


