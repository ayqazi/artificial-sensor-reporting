# Databricks notebook source
class Mounter:
    MOUNTS = {
        "com-cloudership-eu-west-1-databricks-training-sources": "sources",
        "com-cloudership-eu-west-1-databricks-training-tables": "tables",
    }
    
    @classmethod
    def mount_all(cls):
        existing_mounts = [mp.mountPoint 
                           for mp in dbutils.fs.mounts()
                           if mp.mountPoint.startswith("/mnt/")]

        for bucket_name, mount_name in cls.MOUNTS.items():
            mount_path = f"/mnt/{mount_name}"
            bucket_path = f"s3a://{bucket_name}"
            if not mount_path in existing_mounts:
                dbutils.fs.ls(bucket_path) # TEST bucket exists and is readable
                dbutils.fs.mount(bucket_path, mount_path)

Mounter.mount_all()

# COMMAND ----------

display(dbutils.fs.mounts())
