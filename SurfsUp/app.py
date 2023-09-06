# Import the dependencies.
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc, text, and_
from datetime import datetime, timedelta

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///SurfsUp/Resources/hawaii.sqlite")
# # reflect an existing database into a new model
Base = automap_base()
#print(Base)
# # reflect the tables
Base.prepare(autoload_with=engine)

# # Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# # Create our session (link) from Python to the DB
session = Session(engine)

# #################################################
# # Flask Setup
# #################################################
app = Flask(__name__)

# #################################################
# # Flask Routes
# #################################################
@app.route("/")
def welcome():
     """List all available api routes."""
     return (
         f"Available Routes:<br/>"
         f"<br/>"
         f"/api/v1.0/precipitation<br/>"
         f"/api/v1.0/stations<br/>"
         f"/api/v1.0/tobs<br/>"
         f"/api/v1.0/start_date<br/>"
         f"/api/v1.0/start_date/end_date<br/>"
     )

@app.route("/api/v1.0/precipitation")
def precipitations():
    session = Session(engine)

    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    most_recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
    one_year_prior = most_recent_date - timedelta(days=366)
    one_year_prior_data = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date >= one_year_prior, Measurement.prcp != None).all()

    session.close()

    prior_year_precipitation = []
    for date, prcp in one_year_prior_data:
        precipitation_dict = {}
        precipitation_dict["Date"] = date
        precipitation_dict["Precipitation"] = prcp
        prior_year_precipitation.append(precipitation_dict)

    return jsonify(prior_year_precipitation)

@app.route("/api/v1.0/stations")
def stations():
    conn = engine.connect()
    session = Session(engine)
    all_stations_query = text("SELECT DISTINCT station FROM station")
    all_stations = conn.execute(all_stations_query).fetchall()

    session.close()

    stations = []
    for row in all_stations:
        station_dict = {}
        station_dict["Station"] = row['station']
        stations.append(station_dict)
    
    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def temps():
    most_active_station_most_recent_date = session.query(func.max(Measurement.date)).filter(Measurement.station == 'USC00519281').scalar()
    most_active_station_most_recent_date = datetime.strptime(most_active_station_most_recent_date, '%Y-%m-%d')
    most_active_station_one_year_prior = most_active_station_most_recent_date - timedelta(days=366)
    most_active_station_one_year_prior_data = session.query(Measurement.date, Measurement.tobs).\
    filter(Measurement.date >= most_active_station_one_year_prior, Measurement.tobs != None, Measurement.station == 'USC00519281').all()

    session.close()

    prior_year_temperature = []
    for date, tobs in most_active_station_one_year_prior_data:
        temperature_dict = {}
        temperature_dict["Date"] = date
        temperature_dict["Temperature"] = tobs
        prior_year_temperature.append(temperature_dict)

    return jsonify(prior_year_temperature)

@app.route("/api/v1.0/<start>")
def start_date_filter(start):
    start_date = datetime.strptime(start, '%Y-%m-%d')
    temp_data = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))\
    .filter(Measurement.date > start_date - timedelta(days=1)).all()
    first_date = session.query(func.min(Measurement.date)).scalar()
    first_date = datetime.strptime(first_date, '%Y-%m-%d')
    last_date = session.query(func.max(Measurement.date)).scalar()
    last_date = datetime.strptime(last_date, '%Y-%m-%d')

    session.close()

    if start_date < first_date:
        return jsonify ({"Error": f"Start date is before first available date. First available date is {first_date}."}), 404
    
    if start_date > last_date:
        return jsonify ({"Error": f"Start date is after last available date. Last available date is {last_date}."}), 404
    
    start_date_filter_data = []
    for TMIN, TMAX, TAVG in temp_data:
        temp_dict = {}
        temp_dict["Minimum Temperature"] = TMIN
        temp_dict["Maximum Temperature"] = TMAX
        temp_dict["Average Temperature"] = TAVG
        start_date_filter_data.append(temp_dict)

    return jsonify(start_date_filter_data)

@app.route("/api/v1.0/<start>/<end>")
def start_end_date_filter(start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')
    start_end_temp_data = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))\
    .filter(and_(Measurement.date > start_date - timedelta(days=1), Measurement.date <= end_date)).all()
    first_date = session.query(func.min(Measurement.date)).scalar()
    first_date = datetime.strptime(first_date, '%Y-%m-%d')
    last_date = session.query(func.max(Measurement.date)).scalar()
    last_date = datetime.strptime(last_date, '%Y-%m-%d')

    session.close()

    if start_date < first_date:
        return jsonify ({"Error": f"Start date is before first available date. First available date is {first_date}."}), 404
    
    if start_date > last_date:
        return jsonify ({"Error": f"Start date is after last available date. Last available date is {last_date}."}), 404
    
    if start_date > end_date:
        return jsonify ({"Error": f"Start date cannot be after end date."}), 404
    
    if end_date > last_date:
        return jsonify ({"Error": f"End date is after last available date. Last available date is {last_date}."}), 404
    
    start_end_date_filter_data = []
    for TMIN, TMAX, TAVG in start_end_temp_data:
        temp_dict = {}
        temp_dict["Minimum Temperature"] = TMIN
        temp_dict["Maximum Temperature"] = TMAX
        temp_dict["Average Temperature"] = TAVG
        start_end_date_filter_data.append(temp_dict)

    return jsonify(start_end_date_filter_data)

if __name__ == "__main__":
    app.run(debug=True)