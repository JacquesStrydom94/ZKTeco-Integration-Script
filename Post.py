import sqlite3
import json
import requests
from datetime import datetime
import time
import logging

class PostScript:
    def __init__(self, config_file='Log.json', db_name='PUSH.db'):
        self.config = self.load_config(config_file)
        self.DBID = self.config[0].get("DBID")
        self.Token = self.config[1].get("Token")
        self.db_name = db_name
        self.last_processed_id = 0

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return json.load(file)

    def log_posting_json_sql(self, id, zk_id, in_or_out, attype, device, sn, devrec, response_status, response_text):
        timestamp = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        log_entry = {
            "Posting JSON SQL ID": {
                "ZKID": zk_id,
                "Timestamp": timestamp,
                "InorOut": in_or_out,
                "attype": attype,
                "Device": device,
                "SN": sn,
                "Devrec": devrec
            },
            "HTTP Status Code": response_status,
            "Response Text": response_text,
            "Message": f"Successfully updated record with id {id}"
        }
        logging.info(json.dumps(log_entry, indent=4))

    def fetch_new_records(self):
        # Connect to the SQLite database to fetch new records
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM attendance WHERE id > ? AND RESPONSE IS NULL", (self.last_processed_id,))
        records = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        conn.close()
        return records, column_names

    def post_and_update_records(self):
        while True:
            records, column_names = self.fetch_new_records()

            for record in records:
                # Convert the record to a dictionary, skipping certain columns
                record_dict = {column: value for column, value in zip(column_names, record) if column not in ["string", "id", "FTID", "RESPONSE", "KEY"]}

                # Ensure the timestamp format is "2024/10/22 22:11:00"
                for key, value in record_dict.items():
                    if isinstance(value, str) and '-' in value and ':' in value:
                        try:
                            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                            record_dict[key] = dt.strftime("%Y/%m/%d %H:%M:%S")
                        except ValueError:
                            pass
                
                # Serialize the record to JSON
                record_json = json.dumps(record_dict)
                
                # Log the JSON structure
                logging.info(f"Posting JSON SQL ID {record[column_names.index('id')]}: {record_json}")
                
                # Post the JSON data to the API endpoint with authentication
                try:
                    response = requests.post(
                        f'https://appnostic.dbflex.net/secure/api/v2/{self.DBID}/{self.Token}/ZK_stage/create.json',
                        data=record_json,
                        headers={
                            'Content-Type': 'application/json',
                            'Authorization': f'Bearer {self.Token}'
                        }
                    )
                    logging.info(f"HTTP Status Code: {response.status_code}")
                    logging.info(f"Response Text: {response.text}")
                    
                    # If the API response status code is 200, update the record in the database
                    if response.status_code == 200:
                        response_data = response.json()[0]
                        try:
                            # Reconnect to the SQLite database to update the record
                            conn = sqlite3.connect(self.db_name)
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE attendance 
                                SET RESPONSE = ?, KEY = ?, FTID = ? 
                                WHERE id = ?
                            """, (response_data['status'], response_data['key'], response_data['id'], record[column_names.index('id')]))
                            conn.commit()
                            conn.close()
                            
                            logging.info(f"Successfully updated record with id {record[column_names.index('id')]}: RESPONSE={response_data['status']}, KEY={response_data['key']}, FTID={response_data['id']}")
                            
                            # Log the successful post
                            self.log_posting_json_sql(record[column_names.index('id')], record_dict.get("ZKID"), record_dict.get("InorOut"), record_dict.get("attype"), record_dict.get("Device"), record_dict.get("SN"), record_dict.get("Devrec"), response.status_code, response.text)
                            self.last_processed_id = record[column_names.index('id')]
                        except sqlite3.Error as e:
                            logging.error(f"Failed to update record with id {record[column_names.index('id')]}: {e}")
                        finally:
                            conn.close()
                except requests.exceptions.RequestException as e:
                    logging.error(e)
            
            # Wait for a short period before checking for new records again
            time.sleep(10)

    def run(self):
        self.post_and_update_records()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    post_script = PostScript()
    post_script.run()
