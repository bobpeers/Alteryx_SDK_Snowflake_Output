"""
AyxPlugin (required) has-a IncomingInterface (optional).
Although defining IncomingInterface is optional, the interface methods are needed if an upstream tool exists.
"""

import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
from reserved import reserved_words
import time
import csv
import os
import gzip
import multiprocessing
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import snowflake.connector
import logging

VERSION = '1.5'


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi", the Alteryx engine will expect the below five interface methods to be defined.
    """

    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        """
        Constructor is called whenever the Alteryx engine wants to instantiate an instance of this plugin.
        :param n_tool_id: The assigned unique identification for a tool instance.
        :param alteryx_engine: Provides an interface into the Alteryx engine.
        :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
        """

        # Default properties
        self.n_tool_id = n_tool_id
        self.alteryx_engine = alteryx_engine

        # Basic lists of text inputs
        self.input_list: list = ['account', 'user', 'password', 'warehouse', 'database', 'schema', 'table']

        # Create text box variables
        for item in self.input_list:
            setattr(AyxPlugin, item, None)

        self.auth_type: str = None
        self.okta_url: str = None
        self.sql_type: str = None
        self.temp_dir: str = None
        self.key: str = None
        self.case_sensitive: bool = False

        self.is_initialized: bool = True
        self.single_input = None

        # Alteryx to Snowfake data type mappings
        self.var_type: dict = {}
        self.var_type['bool'] = 'BOOLEAN'
        self.var_type['byte'] = 'NUMBER(3, 0)'
        self.var_type['date'] = 'DATE'
        self.var_type['datetime'] = 'TIMESTAMP_NTZ(9)'
        self.var_type['double'] = 'DOUBLE'
        self.var_type['fixeddecimal'] = 'NUMBER'
        self.var_type['float'] = 'FLOAT'
        self.var_type['int16'] = 'NUMBER'
        self.var_type['int32'] = 'NUMBER'
        self.var_type['int64'] = 'NUMBER'
        self.var_type['string'] = 'VARCHAR'
        self.var_type['time'] = 'TIME'
        self.var_type['v_string'] = 'VARCHAR'
        self.var_type['v_wstring'] = 'VARCHAR'
        self.var_type['wstring'] = 'VARCHAR'


    def pi_init(self, str_xml: str):
        """
        Handles input data verification and extracting the user settings for later use.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """
        # stop workflow is output tools are disbaled in runtime settings
        if self.alteryx_engine.get_init_var(self.n_tool_id, 'DisableAllOutput') == 'True':
            self.is_initialized = False
            return False

        # Getting the user-entered file path string from the GUI, to use as output path.
        root = Et.fromstring(str_xml)

        # Basic text inpiut list
        for item in self.input_list:
            setattr(AyxPlugin, item, root.find(item).text if item in str_xml else None)

        self.auth_type = root.find('auth_type').text  if 'auth_type' in str_xml else None
        self.okta_url = root.find('okta_url').text  if 'okta_url' in str_xml else None
        self.temp_dir = root.find('temp_dir').text  if 'temp_dir' in str_xml else None
        self.sql_type = root.find('sql_type').text  if 'sql_type' in str_xml else None
        self.key = root.find('key').text  if 'key' in str_xml else None
        self.case_sensitive = root.find('case_sensitive').text == 'True' if 'case_sensitive' in str_xml else False

        # check for okta url is using okta
        if self.auth_type == 'okta':
            if not self.okta_url:
                self.display_error_msg(f"Enter a valid Okta URL when authenticating using Okta")
                return False 
            elif 'http' not in self.okta_url:
                self.display_error_msg(f"Supplied Okta URL is not valid")
                return False        

        # Check key is selected
        if self.sql_type == 'update' and not self.key:
            self.display_error_msg(f"Please select a valid update key")
            return False

        # data checks
        for item in self.input_list:
            attr = getattr(AyxPlugin, item, None)
            if attr is None:
                self.display_error_msg(f"Enter a valid {item}")
                return False

        # remove protocol if added
        if '//' in self.account:
            self.account = self.account[self.account.find('//') + 2:]

        # Password field
        self.decrypt_password: str = self.alteryx_engine.decrypt_password(self.password, 0)

        # Check temp_dir and use Alteryx default if None
        if not self.temp_dir:
            self.temp_dir = self.alteryx_engine.get_init_var(self.n_tool_id, "TempPath")
            self.display_file(f'{self.temp_dir}| Using system temp dir {self.temp_dir}')
        else:
            error_msg = self.msg_str(self.temp_dir)
            if error_msg != '':
                self.display_error_msg(error_msg)
                return False

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object.
        """

        self.single_input = IncomingInterface(self)
        return self.single_input

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: The name of the output connection anchor, defined in the Config.xml file.
        :return: True signifies that the connection is accepted.
        """

        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        """
        Called when a tool has no incoming data connection.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """

        self.display_error_msg('Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        pass

    def display_error_msg(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, msg_string)
        self.is_initialized = False

    def display_info(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, msg_string)

    def display_file(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.Status.file_output, msg_string)

    @staticmethod
    def write_lists_to_csv(file_path: str, field_lists: list):
        """
        A non-interface, helper function that handles writing to csv and clearing the list elements.
        :param file_path: The file path and file name input by user.
        :param field_lists: The data for all fields.
        """

        with open(file_path, 'a+', encoding='utf-8', newline='') as output_file:
            csv.writer(output_file, delimiter='|', quoting=csv.QUOTE_ALL).writerows(zip(*field_lists))
        for sublist in field_lists:
            del sublist[:]

    @staticmethod
    def msg_str(file_path: str) -> str:
        """
        A non-interface, helper function that handles validating the file path input.
        :param file_path: The file path and file name input by user.
        :return: The chosen message string.
        """

        msg_str = ''
        if len(file_path) > 259:
            msg_str = 'Maximum path length is 259'
        elif any((char in set('/;?*"<>|')) for char in file_path):
            msg_str = 'These characters are not allowed in the file path: /;?*"<>|'
        elif not os.access(file_path, os.W_OK):
            msg_str = 'Unable to write to supplied temp path'
        return msg_str  
    
    @staticmethod
    def gzip(file_path: str):
        '''
        Gzip supplied file and delete original
        '''

        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096*4096), b''):
                with gzip.open(file_path +'.gz', 'ab',compresslevel=3) as w:
                    w.write(chunk)
        os.remove(file_path)

    def create_sql(self, key: str, data_type: str, size: int, scale: int) -> str:
        '''
        Generates Snowflake data type from Alteryx data type.
        Reduces max length if neccessary and adds key field for
        create statements
        '''

        if size > 16777216:
            size = 16777216
        snow_type = self.var_type.get(data_type, 'VARCHAR (16777216)')
        field: str = ''
        if snow_type == 'VARCHAR':
            field = f'{key} {snow_type} ({size})'
        elif data_type == 'fixeddecimal':
            field = f'{key} {snow_type} ({size}, {scale})'
        else:
            field = f'{key} {snow_type}'
        return f'{field} {"NOT NULL" if key == self.key and self.sql_type == "create" else ""}'

class IncomingInterface:
    """
    This optional class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to
    be utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent: object):
        """
        Constructor for IncomingInterface.
        :param parent: AyxPlugin
        """

        # Default properties
        self.parent = parent

        # Custom membersn
        self.record_info_in = None
        self.field_lists: list = []
        self.sql_list: dict = {}
        self.headers: list = []
        self.counter: int = 0
        self.timestamp: int = 0
        self.file_counter: int = 0
        self.file_size_limit: int = 25000000
        self.cache_size: int = 100000

    def get_file_name(self, root: str, base_name: str, counter: int) -> str:
        return os.path.join(root, f'{base_name}{counter}.csv')

    def ii_init(self, record_info_in: object) -> bool:
        """
        Handles the storage of the incoming metadata for later use.
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """
        field_name: str = ''

        if self.parent.alteryx_engine.get_init_var(self.parent.n_tool_id, 'UpdateOnly') == 'True' or not self.parent.is_initialized:
            return False

        self.parent.display_info(f'Running Snowflake Output version {VERSION}')
        self.record_info_in = record_info_in  # For later reference.

        # Storing the field names to use when writing data out.
        for field in range(record_info_in.num_fields):
            field_name = reserved_words(record_info_in[field].name, self.parent.case_sensitive)
            self.field_lists.append([field_name])
            self.headers.append(field_name)
            self.sql_list[field_name] = (str(record_info_in[field].type), record_info_in[field].size, record_info_in[field].scale)

        self.timestamp = str(int(time.time()))
        self.parent.temp_dir = os.path.join(self.parent.temp_dir, self.timestamp)

        # Create filepaths when running
        self.csv_file = self.get_file_name(self.parent.temp_dir, self.parent.table, self.file_counter)
        
        path = os.path.dirname(self.csv_file)

        if not os.path.exists(os.path.abspath(path)):
            os.makedirs(os.path.abspath(path))
        
        # Logging setup
        logging.basicConfig(filename=os.path.join(path, 'snowflake_connector.log'), format='%(asctime)s - %(message)s', level=logging.INFO)

        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for writing the data to csv in chunks.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False if file path string is invalid, otherwise True.
        """

        self.counter += 1  # To keep track for chunking

        if not self.parent.is_initialized:
            return False

        # Storing the string data of in_record
        for field in range(self.record_info_in.num_fields):
            in_value = self.record_info_in[field].get_as_string(in_record)
            self.field_lists[field].append(in_value) if in_value is not None else self.field_lists[field].append('NULL')

        # Writing when chunk mark is met
        if self.counter % self.cache_size == 0:
            self.parent.write_lists_to_csv(self.csv_file, self.field_lists)

            # Start new csv file if limit reached
            if self.counter % (self.file_size_limit) == 0:
                self.file_counter += 1
                # create new file name
                self.csv_file = self.get_file_name(self.parent.temp_dir, self.parent.table, self.file_counter)

                # append headers for new file
                for record in range(0, len(self.headers)):
                    self.field_lists[record].append(self.headers[record])

        return True
      

    def ii_update_progress(self, d_percent: float):
        """
         Called by the upstream tool to report what percentage of records have been pushed.
         :param d_percent: Value between 0.0 and 1.0.
        """

        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)  # Inform the Alteryx engine of the tool's progress

    def ii_close(self):
        """
        Handles writing out any residual data out.
        Called when the incoming connection has finished passing all of its records.
        """
        
        if self.parent.alteryx_engine.get_init_var(self.parent.n_tool_id, 'UpdateOnly') == 'True' or not self.parent.is_initialized:
            return False
        elif self.counter == 0:
            self.parent.display_info('No records to process')
            return False

        con: snowflake.connector.connection = None

        # Outputting the link message that the file was written
        if len(self.csv_file) > 0 and self.parent.is_initialized:
            # First element for each list will always be the field names.
            if len(self.field_lists[0]) > 1:
                self.parent.write_lists_to_csv(self.csv_file, self.field_lists)

        # gzip files
        files = [os.path.join(self.parent.temp_dir, f) for f in os.listdir(self.parent.temp_dir) if self.parent.table in f]

        #self.parent.gzip(self.csv_file)
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()*10) as executor:
            executor.map(self.parent.gzip, files) 

        for f in files:
            f = os.path.join(self.parent.temp_dir, f)
            self.parent.display_file(f'{f} | {f} gzip file is created')

        # clean key field
        if self.parent.key:
            self.parent.key = reserved_words(self.parent.key, self.parent.case_sensitive)

        # path to gzip files
        gzip_file = os.path.join(self.parent.temp_dir, self.parent.table).replace('\\', '//')

        # Create Snowflake connection

        try:
            if self.parent.auth_type == 'snowflake':
                con = snowflake.connector.connect(
                                                user=self.parent.user,
                                                password=self.parent.decrypt_password,
                                                account=self.parent.account,
                                                warehouse=self.parent.warehouse,
                                                database=self.parent.database,
                                                schema=self.parent.schema,
                                                ocsp_fail_open=True
                                                )
                self.parent.display_info('Authenticated via Snowflake')
            else:
                con = snowflake.connector.connect(
                                                user=self.parent.user,
                                                password=self.parent.decrypt_password,
                                                authenticator=self.parent.okta_url,
                                                account=self.parent.account,
                                                warehouse=self.parent.warehouse,
                                                database=self.parent.database,
                                                schema=self.parent.schema,
                                                ocsp_fail_open=True
                                                )                
                self.parent.display_info('Authenticated via Okta')

            # fix table name if case sensitive used or keyswords
            self.parent.table = reserved_words(self.parent.table, self.parent.case_sensitive)
        
            # Execute Table Creation #
            if self.parent.sql_type in ('create', 'update'):
                if self.parent.sql_type == 'create':
                    table_sql: str = f"Create or Replace table {self.parent.table}  ({', '.join([self.parent.create_sql(k,v,s,c) for k, (v,s,c) in self.sql_list.items()])}"

                elif self.parent.sql_type == 'update':
                    table_sql: str = f"Create or Replace TEMPORARY TABLE tmp  ({', '.join([self.parent.create_sql(k,v,s,c) for k, (v,s,c) in self.sql_list.items()])}"

                table_sql += f', PRIMARY KEY ({self.parent.key}))' if self.parent.key else ')'

                con.cursor().execute(table_sql)

            # PUT and COPY to Snowflake

            if self.parent.sql_type == 'truncate':
                con.cursor().execute(f'truncate table {self.parent.table}')

            if self.parent.sql_type in ('create', 'truncate', 'append'):
                con.cursor().execute("PUT 'file://{0}*' @%{1} PARALLEL=64 OVERWRITE=TRUE".format(gzip_file, self.parent.table))
                con.cursor().execute("""COPY INTO {0} FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF='NULL' COMPRESSION=GZIP SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"') PURGE = TRUE""".format(self.parent.table))

            elif self.parent.sql_type == 'update':
                con.cursor().execute("PUT 'file://{0}*' @%tmp PARALLEL=64 OVERWRITE=TRUE".format(gzip_file))
                con.cursor().execute("""COPY INTO tmp FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' NULL_IF='NULL' COMPRESSION=GZIP SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"') PURGE = TRUE""")


                insert_fields = ', '.join(self.sql_list)
                set_fields = ', '.join([f + ' = tmp.' + f for f in self.sql_list])
                tmp_fields = (', ').join(['tmp.' + fld for fld in self.sql_list])

                merge_query = (f'merge into {self.parent.table} '
                                    f'using tmp on {self.parent.table}.{self.parent.key} = tmp.{self.parent.key} '
                                    f'when matched then '
                                    f'update set {set_fields} '
                                    f'when not matched then '
                                    f'insert ({insert_fields}) values ({tmp_fields});')

                con.cursor().execute(merge_query)

        except Exception as e:
            logging.error(str(e))
            self.parent.display_error_msg(f'Error {e.errno} ({e.sqlstate}): {e.msg} ({e.sfqid})')
        finally:
            if con:
                con.close()
        self.parent.display_info(f'Processed {self.counter:,} records')
        self.parent.display_info('Snowflake transaction complete')