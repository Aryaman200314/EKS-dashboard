import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from datetime import datetime, timezone, timedelta
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Kubernetes Imports
from kubernetes import client
from kubernetes.config.kube_config import KubeConfigLoader
from kubernetes.client.rest import ApiException

from aws_data_fetcher import get_session
print("test")
session = get_session("arn:aws:iam::858259715211:role/EKS-access-role-for-dashboard-custom")
print(f"sessiosn122-------------------{session}")
