FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano netcdf-bin
RUN pip install netCDF4 pymongo xarray numpy geopy

WORKDIR /app
COPY *.py .
COPY *.sh .
COPY parameters/basinmask_01.nc parameters/basinmask_01.nc
RUN chown -R 1000660000 /app
