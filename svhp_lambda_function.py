import json
import os
from pymongo import MongoClient
from datetime import datetime, timedelta


MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", "")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "userMetadata")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

def create_user_document(event_data):
    """
    Creates a document in the required format from Kafka event data.
    """
    user_id = event_data.get("user_id", "unknown_user")

    # Simulated device and app data extraction from event_data
    user_aggregate_data = event_data.get("user_aggregate_data", {})
    device_info = {
        "user_user_device_brand": "",
        "user_user_device_model": "",
        "user_user_device_platform": "",
        "user_wallet_balance_bucket_78": "",
    }
    if user_aggregate_data:
        device_info = {
            "user_user_device_brand": str(user_aggregate_data.get("device_brand", "")),
            "user_user_device_model": str(user_aggregate_data.get("device_model", "")),
            "user_user_device_platform": str(user_aggregate_data.get("device_platform", "")),
            "user_wallet_balance_bucket_78": user_aggregate_data.get("wallet_balance", 0),
        }
    other_app_info = event_data.get("other_app_info", {})

    app_info = {
            "com_application_zomato": "",
            "com_facebook_katana": "",
            "com_flipkart_android": "",
            "com_grofers_customerapp": "",
            "com_instagram_android": "",
            "com_linkedin_android": "",
            "com_phonepe_app": "",
            "com_whatsapp": "",
            "in_amazon_mshop_android_shopping": "",
            "in_swiggy_android": "",
            "net_one97_paytm": "",
            "com_zerodha_kite3": "",
            "com_twitter_android": "",
    }
    if other_app_info:
        app_info = {
            "com_application_zomato": str(other_app_info.get("com.application.zomato", "")),
            "com_facebook_katana": str(other_app_info.get("com.facebook.katana", "")),
            "com_flipkart_android": str(other_app_info.get("com.flipkart.android", "")),
            "com_grofers_customerapp": str(other_app_info.get("com.grofers.customerapp", "")),
            "com_instagram_android": str(other_app_info.get("com.instagram.android", "")),
            "com_linkedin_android": str(other_app_info.get("com.linkedin.android", "")),
            "com_phonepe_app": str(other_app_info.get("com.phonepe.app", "")),
            "com_whatsapp": str(other_app_info.get("com.whatsapp", "")),
            "in_amazon_mshop_android_shopping": str(other_app_info.get("in.amazon.mShop.android.shopping", "")),
            "in_swiggy_android": str(other_app_info.get("in.swiggy.android", "")),
            "net_one97_paytm": str(other_app_info.get("net.one97.paytm", "")),
            "com_zerodha_kite3": str(other_app_info.get("net.one97.paytm", "")),
            "com_twitter_android": str(other_app_info.get("com.twitter.android", "")),
        }


    # Structure the document
    document = {
        "_id": user_id,
        "user_aggregate": device_info,
        "other_app_info": app_info,
    }

    return document

def lambda_handler(event, context):
    """
    Lambda function to process SVHP events and insert data into MongoDB.
    """
    try:
        for record_key, records in event["records"].items():
            for message in records:
                # Decode the Kafka event message payload
                payload = json.loads(message["value"])

                # Generate the document for MongoDB
                document = create_user_document(payload)

                # Upserting should be done here
                collection.insert_one(document)
                print(f"Inserted document for user ID: {document['_id']}")

        return {
            "statusCode": 200,
            "body": json.dumps("Successfully processed SVHP events.")
        }
    except Exception as e:
        print(f"Error processing SVHP events: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps("Error processing SVHP events.")
        }
