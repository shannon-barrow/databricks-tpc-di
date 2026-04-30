# Databricks notebook source
from datetime import date, timedelta, datetime

batch_date_ls = []
start_date    = datetime(2015, 7, 6)
for dt_interval in range(0, 700):
  batch_date_ls.append((start_date + timedelta(days=dt_interval)).strftime("%Y-%m-%d"))
dbutils.jobs.taskValues.set(key = "batch_date_ls", value = batch_date_ls)