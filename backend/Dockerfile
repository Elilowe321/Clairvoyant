# Base docker file to install all requirements (set up requirements as I go)

FROM python:3.11.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app