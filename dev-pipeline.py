# Databricks notebook source
small_sample = spark.read.json("/mnt/sources/artificial-sensor-reporting/dev/small-sample.json")
display(small_sample)

