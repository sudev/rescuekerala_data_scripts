import requests
import json
import psycopg2
import logging
from psycopg2.extras import execute_values


class DBImporter(object):
    def __init__(self):
        self.data_cache = {}

    def __pull_records_api(self, api_endpoint, api_offset=0):
        """
        Returns a json for the given endpoint and api_offset.
        :param api_endpoint:
        :param api_offset:
        :return:
        """
        try:
            api_endpoint = "{api_endpoint}?offset={api_offset}".format(
                api_endpoint=api_endpoint, api_offset=api_offset)
            data = requests.get(api_endpoint).json()
            print(data['meta'])
            return data['data']
        except Exception as e:
            # TODO - Request retries and exceptions which can be handled.
            print(e)
            raise e

    def iter_api_records(self, api_enpoint, api_offset, api_limit):
        has_data = True
        while has_data:
            data_json_list = self.__pull_records_api(api_enpoint, api_offset)
            if any(data_json_list):
                # you may store, pickle or push data to database after some pre-processing.
                # self.process_data(data_json_list)
                for rec in data_json_list:
                    yield rec
                api_offset += api_limit
            else:
                has_data = False

    def process_data(self, api_enpoint, api_offset, api_limit):
        """
        Starting point if you want to call an API and write data to db/disk.
        :return:
        """
        batch_records = []
        # This function should be overridden for each use case.
        for pos, rec in enumerate(
                self.iter_api_records(api_enpoint, api_offset, api_limit)):
            batch_records.append(rec)
            print(pos)
            # Do whatever manipulation you want with the record before inserting into db.
            if (len(batch_records) > 1000):
                # Write to a file/files.
                self.flush_to_file(batch_records, "/tmp/data.json")
                batch_records = []
            self.flush_to_file(batch_records, "/tmp/data.json")

    def flush_to_file(self, records, file_path):
        with open(file_path, 'a') as fh:
            fh.writelines([json.dumps(rec) + "\n" for rec in records])

    def write_to_postgres(self, records, insert_query, db_confs):
        """
        Writes to postgres using psycog, not too efficient right now.
        Probably look for copy command if you access to db.
        :param records:
        :param insert_query:
        :param db_confs:
        :return:
        """
        try:
            connect_str = "dbname='{db_name}' user='{user_name}' host='{host_name}' ".format(
                **db_confs)
            import pdb
            pdb.set_trace()
            if "user_password" in db_confs:
                connect_str = connect_str + "password='{user_password}".format(
                    user_password=db_confs["user_password"])
            with psycopg2.connect(connect_str) as connection:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_values(
                        cursor,
                        insert_query, [[rec] for rec in records],
                        template=None,
                        page_size=100)
        except Exception as e:
            logging.error("Error inserting records into postgres.")
            raise (e)


if __name__ == '__main__':
    dbimp = DBImporter()
    """
    
    # Process data from an endpoint.
    dbimp.process_data(
        api_enpoint="https://keralarescue.in/relief_camps/data",
        api_offset=4000,
        api_limit=300)
    
    db_confs = {
        "db_name": "test",
        "user_name": "sudev.ac",
        "host_name": "localhost"
    }
    insert_command = "INSERT INTO test_table (things) VALUES %s"
    dbimp.write_to_postgres([1, 2], insert_command, db_confs)
    """
