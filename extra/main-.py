#main gpudb class functions: 


to build aMCP server for kinetica, think about which of these would be helpful (mcp server that has tools that an ai assistant can access to do stuff with kinetica, so like querying, aggregagres, showing databases, showing tables etc)


# ---------------------------------------------------------------------------
# GPUdb - Lightweight client class to interact with a GPUdb server.
# ---------------------------------------------------------------------------

class GPUdb(object):

    """
    This is the main class to be used to provide the client functionality to
    interact with the server.

    Usage patterns

    * Secured setup (Default)

      This code given below will set up a secured connection. The property 'skip_ssl_cert_verification'
      is set to 'False' by default. SSL certificate check will be enforced by default.

      ::

          options = GPUdb.Options()
          options.username = "user"
          options.password = "password"
          options.logging_level = "debug"

          gpudb = GPUdb(host='https://your_server_ip_or_FQDN:8082/gpudb-0', options=options )

    * Unsecured setup

      The code given below will set up an unsecured connection to the server. The property 'skip_ssl_cert_verification'
      has been set explicitly to 'True'. So, irrespective of whether an SSL setup is there or not all certificate checks
      will be bypassed.

      ::

          options = GPUdb.Options()
          options.username = "user"
          options.password = "password"
          options.skip_ssl_cert_verification = True
          options.logging_level = "debug"

          gpudb = GPUdb(host='https://your_server_ip_or_FQDN:8082/gpudb-0', options=options )

      Another way of setting up an unsecured connection is as given by the code below. In this case, the URL
      is not a secured one so no SSL setup comes into play.

      ::

          options = GPUdb.Options()
          options.username = "user"
          options.password = "password"
          options.logging_level = "debug"

          gpudb = GPUdb(host='http://your_server_ip_or_FQDN:9191', options=options )
    """

    # Logging related string constants
    # Note that the millisecond is put in the message format due to a shortcoming
    # of the python datetime format shortcoming
    _LOG_MESSAGE_FORMAT  = "%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s"
    _LOG_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    # Headers that are protected and cannot be overridden by users
    _protected_headers = [
        C._HEADER_ACCEPT,
        C._HEADER_AUTHORIZATION,
        C._HEADER_CONTENT_TYPE,
        C._HEADER_HA_SYNC_MODE
    ]




    # -------------------------  GPUdb Members --------------------------------
    __http_response_triggering_failover = [
        httplib.SERVICE_UNAVAILABLE,   # most likely
        httplib.INTERNAL_SERVER_ERROR,
        httplib.GATEWAY_TIMEOUT,
        httplib.BAD_GATEWAY            # rank-0 killed with HTTPD gives this
    ]
    __endpoint_server_error_magic_strings = [
        C._DB_EXITING_ERROR_MESSAGE,
        C._DB_OFFLINE_ERROR_MESSAGE,
        C._DB_SYSTEM_LIMITED_ERROR_MESSAGE,
        C._DB_CONNECTION_REFUSED,
        C._DB_CONNECTION_RESET
    ]

    # Default host manager port for http and httpd
    _DEFAULT_HOST_MANAGER_PORT       = 9300
    _DEFAULT_HTTPD_HOST_MANAGER_PORT = 8082
    _DEFAULT_FAILBACK_POLLING_INTERVAL = 5

    # The timeout (in seconds) used for checking the status of a node; we used
    # to use a small timeout so that it does not take a long time to figure out
    # that a rank is down, but connections over high-traffic networks or the
    # cloud may encounter significant connection wait times.  Using 20 seconds.
    # __DEFAULT_INTERNAL_ENDPOINT_CALL_TIMEOUT = 20

    _DEFAULT_SERVER_CONNECTION_TIMEOUT = 5  # in seconds

    # The number of times that the API will attempt to submit a host
    # manager endpoint request.  We need this in case the user chose
    # a bad host manager port.  We don't want to go into an infinite
    # loop
    __HOST_MANAGER_SUBMIT_REQUEST_RETRY_COUNT = 3

    __SSL_ERROR_MESSAGE_TEMPLATE = (
        "<{}>.  "
        "To fix, either:  "
        "1) Add the server's certificate or a CA cert to the system CA certificates file, or "
        "2) Skip the certificate check using the skip_ssl_cert_verification option.  "
        "Examples:  https://docs.kinetica.com/7.2/api/concepts/#https-without-certificate-validation"
    )

    END_OF_SET = -9999
    """(int) Used for indicating that all of the records (till the end of the
    set are desired)--generally used for /get/records/* functions.
    """

    # The version of this API
    api_version = "7.2.2.7"

    # -------------------------  GPUdb Methods --------------------------------

    def __init__( self, host = None, options = None, *args, **kwargs ):
        """
        Construct a new GPUdb client instance.  This object communicates to
        the database server at the given address.  This class implements
        HA failover, which means that upon certain error conditions, this class
        will try to establish connection with one of the other clusters
        (specified by the user or known to the ring) to continue service.
        There are several options related to how to control that in the
        :class:`GPUdb.Options` class that can be controlled via `options`.

        .. note::

            Please read the docstring of `options` about backward-
            compatibility related notes.

        Parameters:

            host (str or list of str)
                The URL(s) of the GPUdb server. May be provided as a comma
                separated string or a list of strings containing head or worker
                rank URLs of the server clusters.  Must be full and valid URLs.
                Example: "https://domain.com:port/path/".
                If only a single URL or host is given, and no *primary_host* is
                explicitly specified via the options, then the given URL will be
                used as the primary URL.  Default is 'http://127.0.0.1:9191'
                (implemented internally).

                Note that in versions 7.0 and prior, the URL also allowed
                username:password@ in front of the hostname.  That is now
                deprecated.  For now, anything in the hostname separated
                by the @ symbol will be discarded.  (But the constructor
                will still function).  Please use the appropriate properties
                of the `options` argument to set the username and
                password.

            options (GPUdb.Options or dict)
                Optional arguments for creating this GPUdb object.  To be
                backward compatible to 7.0 versions, keyword arguments will
                be honored (only if no options is given).  I.e., if options
                is given, no positional or keyword argument can be given.  See
                :class:`Options` for all available properties.

                .. seealso:: :class:`GPUdb.Options`
    """

    def get_host( self ):
        """Return the host this client is talking to."""
        return self.get_url( stringified = False ).host
    # end get_host


    def get_primary_host( self ):
        """Return the primary host for this client."""
        return self.__primary_host
    # end get_primary_host



    def set_primary_host( self, new_primary_host,
                          start_using_new_primary_host = False,
                          delete_old_primary_host      = False ):
        """Set the primary host for this client.  Start using this host
        per the user's directions.  Also, either delete any existing primary
        host information, or relegate it to the ranks of a backup host.

        Parameters:
            value (str)
                A string containing the full URL of the new primary host (of
                the format 'http[s]://X.X.X.X:PORT[/httpd-name]').  Must have
                valid URL format.  May be part of the given back-up hosts, or
                be a completely new one.

            start_using_new_primary_host (bool)
                Boolean flag indicating if the new primary host should be used
                starting immediately.  Please be cautious about setting the value
                of this flag to True; there may be unintended consequences regarding
                query chaining.  Caveat: if values given is False, but
                *delete_old_primary_host* is True and the old primary host, if any,
                was being used at the time of this function call, then the client
                still DOES switch over to the new primary host.  Default value is False.

            delete_old_primary_host (bool)
                Boolean flag indicating that if a primary host was already set, delete
                that information.  If False, then any existing primary host URL would
                treated as a regular back-up cluster's host.  Default value is False.


        .. deprecated:: 7.1.0.0

            As of version 7.1.0.0, this method will no longer be
            functional.  This method will be a no-op, not changing primary host.
            port.  The method will be removed in version 7.2.0.0.  The only
            way to set the primary host is via `GPUdb.Options` at `GPUdb`
            initialization.  It cannot be changed after that.
        """
        pass
    # end set_primary_host


    def get_port( self ):
        """Return the port the host is listening to."""
        return self.get_url( stringified = False ).port
    # end get_port


    def get_host_manager_port( self ):
        """Return the port the host manager is listening to."""
        return self.get_hm_url( stringified = False ).port
    # end get_host_manager_port


    def get_url( self, stringified = True ):
        """Return the GPUdb.URL or its string representation that points to the
        current head node of the current cluster in use.

        Parameters:
            stringified (bool)
                Optional argument.  If True, return the string representation,
                otherwise return the :class:`GPUdb.URL` object.  Default is
                True.

        Returns:
            The :class:`GPUdb.URL` object or its string representation.
        """
        # Ensure we have some cluster information first!
        if not self.__cluster_info:
            raise GPUdbException( "No cluster registered with the API yet!" )

        # Get the current URL
        url = self.current_cluster_info.head_rank_url

        if stringified:
            return str(url)
        else:
            return url
    # end get_url


    def get_hm_url( self, stringified = True ):
        """Return the GPUdb.URL or its string representation that points to the
        current host manager of the current cluster in use.

        Parameters:
            stringified (bool)
                Optional argument.  If True, return the string representation,
                otherwise return the :class:`GPUdb.URL` object.  Default is
                True.

        Returns:
            The :class:`GPUdb.URL` object or its string representation.
        """
        # Get the current URL
        url = self.current_cluster_info.host_manager_url

        if stringified:
            return str(url)
        else:
            return url
    # end get_hm_url


    def get_failover_urls( self ):
        """Return a list of the head node URLs for each of the clusters in the
        HA ring in failover order.

        Returns:
            A list of :class:`GPUdb.URL` objects.
        """
        # Get the current URL

        return [ self.__cluster_info[cluster_index].head_rank_url for cluster_index in self.__cluster_indices ]
    # end get_failover_urls


    def get_head_node_urls( self ):
        """Return a list of the head node URLs for each of the clusters in the
        HA ring for the database server.

        Returns:
            A list of :class:`GPUdb.URL` objects.
        """
        # Get the current URL

        return [ cluster.head_rank_url for cluster in self.__cluster_info ]
    # end get_head_node_urls


    def get_num_cluster_switches( self ):
        """Gets the number of times the client has switched to a different
        cluster amongst the high availability ring.
        """
        return self.__num_cluster_switches
    # end get_num_cluster_switches


    @property
    def current_cluster_info( self ):
        """Return the :class:`GPUdb.ClusterAddressInfo` object
        containing information on the current/active cluster."""
        return self.__cluster_info[ self.__get_curr_cluster_index() ]
    # end all_cluster_info


    @property
    def all_cluster_info( self ):
        """Return the list of :class:`GPUdb.ClusterAddressInfo` objects
        that contain address of each of the clusters in the ring."""
        return self.__cluster_info
    # end all_cluster_info


    @property
    def ha_ring_size( self ):
        """Return the list of :class:`GPUdb.ClusterAddressInfo` objects
        that contain address of each of the clusters in the ring."""
        return len( self.__cluster_info )
    # end all_cluster_info


    @property
    def options( self ):
        """Return the :class:`GPUdb.Options` object that contains all
        the knobs the user can turn for controlling this class's behavior.
        """
        return self.__options
    # end options


    @property
    def host(self):
        return self.get_host()

    @host.setter
    def host(self, value):
        """
        .. deprecated:: 7.1.0.0

            As of version 7.1.0.0, this method will no longer be
            functional.  This method will be a no-op, not changing host.
            The method will be removed in version 7.2.0.0.  The only
            way to set the host at `GPUdb` initialization.  It cannot be
            changed after that.
        """
        pass
    # end host setter

    @property
    def port(self):
        return self.get_port()

    @port.setter
    def port(self, value):
        """
        .. deprecated:: 7.1.0.0

            As of version 7.1.0.0, this method will no longer be
            functional.  This method will be a no-op, not changing port.
            The method will be removed in version 7.2.0.0.  The only
            way to set the port at `GPUdb` initialization.  It cannot be
            changed after that.
        """
        pass
    # end port setter

    @property
    def host_manager_port(self):
        return self.get_host_manager_port()

    @host_manager_port.setter
    def host_manager_port(self, value):
        """
        .. deprecated:: 7.1.0.0

            As of version 7.1.0.0, this method will no longer be
            functional.  This method will be a no-op, not changing host manager
            port.  The method will be removed in version 7.2.0.0.  The only
            way to set the host manager port is via `GPUdb.Options` at `GPUdb`
            initialization.  It cannot be changed after that.
        """
        pass
    # end host_manager_port setter

    @property
    def gpudb_url_path(self):
        return self.get_url( stringified = False ).path

    @gpudb_url_path.setter
    def gpudb_url_path(self, value):
        """
        .. deprecated:: 7.1.0.0

            As of version 7.1.0.0, this method will no longer be
            functional.  This method will be a no-op, not changing URL path
            The method will be removed in version 7.2.0.0.  The only
            way to set the URL path is via `GPUdb.Options` at `GPUdb`
            initialization.  It cannot be changed after that.
        """
        pass
    # end gpudb_url_path setter

    @property
    def gpudb_full_url(self):
        """Returns the full URL of the current head rank of the currently
        active cluster."""
        return self.get_url( stringified = False ).url

    @property
    def server_version(self):
        """Returns the :class:`GPUdb.Version` object representing the version of
        the *currently active* cluster of the Kinetica server."""
        return self.current_cluster_info.server_version

    @server_version.setter
    def server_version(self, value):
        self.current_cluster_info.server_version = value

    @property
    def connection(self):
        return self.__protocol

    @connection.setter
    def connection(self, value):
        """
        .. deprecated:: 7.1.0.0

            As of version 7.1.0.0, this method will no longer be
            functional.  This method will be a no-op, not changing protocol
            The method will be removed in version 7.2.0.0.  The only
            way to set the protocol is via `GPUdb.Options` at `GPUdb`
            initialization.  It cannot be changed after that.
        """
        pass
    # end connection setter

    @property
    def protocol(self):
        """Returns the HTTP protocol being used by the :class:`GPUdb`
        object to communicate to the database server.
        """
        return self.__protocol

    @property
    def primary_host(self):
        """Returns the primary hostname."""
        return self.__primary_host

    @property
    def username(self):
        """Gets the username to be used for authentication to GPUdb."""
        return self.__username

    @property
    def password(self):
        """Gets the password to be used for authentication to GPUdb."""
        return self.__password

    @property
    def oauth_token(self):
        """Gets the OAuth2 token to be used for authentication to GPUdb."""
        return self.__oauth_token

    @property
    def encoding(self):
        return self.__encoding

    @property
    def timeout(self):
        """Gets the timeout used for http connections to GPUdb."""
        return self.__timeout

    @property
    def disable_auto_discovery(self):
        """Returns whether auto-discovery has been disabled."""
        return self.__disable_auto_discovery

    @property
    def ha_sync_mode(self):
        return self._ha_sync_mode

    @ha_sync_mode.setter
    def ha_sync_mode(self, value ):
        if not isinstance( value, GPUdb.HASynchronicityMode ):
            raise GPUdbException( "HA sync mode must be of type '{}', given {}!"
                                  "".format( str( GPUdb.HASynchronicityMode ),
                                             str( type( value ) ) ) )
        # end error checking

        self._ha_sync_mode = value
    # end setter

    @property
    def logging_level(self):
        """Returns the integer value of the logging level that is being used by
        the API.  By default, logging is set to NOTSET, and the logger will
        honor the root logger's level.
        """
        return self.log.level


    @property
    def skip_ssl_cert_verification(self):
        return self.__skip_ssl_cert_check


    def save_known_type(self, type_id, _type ):
        self._known_types[ type_id ] = _type


    @property
    def get_known_types(self):
        """Return all known types; if
        none, return None.
        """
        return self._known_types
    # end get_known_types

    @property
    def host_addresses(self):
        return self.__host_addresses


    def get_known_type(self, type_id, lookup_type = True ):
        """Given an type ID, return any associated known type; if
        none is found, then optionally try to look it up and save it.
        Otherwise, return None.

        Parameters:
            type_id (str)
                The ID for the type.

            lookup_type (bool)
                If True, then if the type is not already found, then
                to look it up by invoking :meth:`.show_types`, save
                it for the future, and return it.

        Returns:
            The associated RecordType, if found (or looked up).  None
            otherwise.
        """
        if type_id in self._known_types:
            return self._known_types[ type_id ]

        if lookup_type:
            # Get the type info from the database
            type_info = self.show_types( type_id = type_id, label = "" )
            if not _Util.is_ok( type_info ):
                raise GPUdbException( "Error in finding type {}: {}"
                                      "".format( type_id,
                                                 _Util.get_error_msg( type_info ) ) )

            # Create the RecordType
            record_type = RecordType.from_type_schema( label = "",
                                                       type_schema = type_info["type_schemas"][ 0 ],
                                                       properties  = type_info["properties"][ 0 ] )

            # Save the RecordType
            self._known_types[ type_id ] = record_type

            return record_type
        # end if

        return None # none found
    # end get_known_type


    def get_all_available_full_urls( self, stringified = True ):
        """Return the list of :class:`GPUdb.URL` objects or its string
        representation that points to the current head node of each of the
        clusters in the ring.

        Parameters:
            stringified (bool)
                Optional argument.  If True, return the string representation,
                otherwise return the :class:`GPUdb.URL` object.  Default is
                True.

        Returns
        """:

        return 


    def encode_datum(self, SCHEMA, datum, encoding = None):
        """
        Returns an Avro binary or JSON encoded datum dict using its schema.

        Parameters:
            SCHEMA (str or avro.Schema)
                A parsed schema object from avro.schema.parse() or a
                string containing the schema.

            datum (dict)
                A dict of key-value pairs containing the data to encode (the
                entries must match the schema).
        """
        # Convert the string to a parsed schema object (if needed)
        if isinstance( SCHEMA, basestring ):
            SCHEMA = schema.parse( SCHEMA )

        if encoding is None:
            encoding = self.encoding
        else:
            encoding = encoding.upper()

        # Build the encoder; this output is where the data will be written
        if encoding == C._ENCODING_BINARY or encoding == C._ENCODING_SNAPPY:
            return _Util.encode_binary_data( SCHEMA, datum, self.encoding )
        elif encoding == C._ENCODING_JSON:
            return json.dumps( _Util.convert_dict_bytes_to_str( datum ) )
    # end encode_datum


    def encode_datum_cext(self, SCHEMA, datum, encoding = None):
        """
        Returns an avro binary or JSON encoded datum dict using its schema.

        Parameters:
            SCHEMA (str or avro.Schema)
                A parsed schema object from avro.schema.parse() or a
                string containing the schema.

            datum (dict)
                A dict of key-value pairs containing the data to encode (the
                entries must match the schema).
        """
        if encoding is None:
            encoding = self.encoding
        else:
            encoding = encoding.upper()

        # Build the encoder; this output is where the data will be written
        if encoding == C._ENCODING_BINARY or encoding == C._ENCODING_SNAPPY:
            return _Util.encode_binary_data_cext( SCHEMA, datum, self.encoding )
        elif encoding == C._ENCODING_JSON:
            # Convert bytes to strings first
            datum = _Util.convert_dict_bytes_to_str( datum )

            # Create an OrderedDict for the JSON since the server expects
            # fields in order
            json_datum = collections.OrderedDict()

            # Populate the JSON-encoded payload
            for field in SCHEMA.fields:
                name = field.name
                json_datum[ name ] = datum[ name ]
            # end loop

            return json.dumps( json_datum )
    # end encode_datum_cext



    # ------------- Convenience Functions ------------------------------------


    @staticmethod
    def valid_json(self, json_string):
        """
        Validates a JSON string by trying to parse it into a Python object
        """
        try:
            json.loads(json_string)
        except ValueError as err:
            return False
        return True

    @staticmethod
    def merge_dicts(self, *dict_args):
        """
        Given any number of dictionaries, shallow copy and merge into a new dict,
        precedence goes to key-value pairs in latter dictionaries.
        """
        result = {}
        for dictionary in dict_args:
            result.update(dictionary)
        return result


    @staticmethod
    def is_json_array(json_string):
        trimmed = json_string.strip()
        return trimmed.startswith("[") and trimmed.endswith("]")


    @staticmethod
    def is_json(json_string):
        try:
            obj = json.loads(json_string)
            return isinstance( obj, list), ''
        except ValueError as err:
            return False, str(err)


    @staticmethod
    def convert_json_list_to_json_array(json_list):
        if not isinstance(json_list, list):
            raise ValueError("Input must be an object of type 'list'")
        return "[{}]".format(",".join(json_list))


    def read_trigger_msg(self, encoded_datum):
        RSP_SCHEMA = self.gpudb_schemas[ "trigger_notification" ]["RSP_SCHEMA"]
        return self.__read_orig_datum_cext(RSP_SCHEMA, encoded_datum, C._ENCODING_BINARY)



    def logger(self, ranks, log_levels, options = {}):
        """Convenience function to change log levels of some
        or all GPUdb ranks.

        Parameters:
            ranks (list of ints)
                A list containing the ranks to which to apply the new log levels.

            log_levels (dict of str to str)
                A map where the keys dictate which log's levels to change, and the
                values dictate what the new log levels will be.

            options (dict of str to str)
                Optional parameters.  Default value is an empty dict ( {} ).

        Returns:
            A dict with the following entries--

            status (str)
                The status of the endpoint ('OK' or 'ERROR').

            log_levels (map of str to str)
                A map of each log level to its respective value
        """
        REQ_SCHEMA = self.logger_request_schema
        RSP_SCHEMA = self.logger_response_schema

        datum = {}
        datum["ranks"]      = ranks
        datum["log_levels"] = log_levels
        datum["options"]    = options

        response = self.__submit_request( "/logger", datum )

        if not _Util.is_ok( response ): # problem setting the log levels
            raise GPUdbException( "Problem setting the log levels: "
                                  + _Util.get_error_msg( response ) )

        return AttrDict( response )
    # end logger


    def set_server_logger_level(self, ranks, log_levels, options = {}):
        """Convenience function to change log levels of some
        or all GPUdb ranks.

        Parameters:
            ranks (list of ints)
                A list containing the ranks to which to apply the new log levels.

            log_levels (dict of str to str)
                A map where the keys dictate which log's levels to change, and the
                values dictate what the new log levels will be.

            options (dict of str to str)
                Optional parameters.  Default value is an empty dict ( {} ).

        Returns:
            A dict with the following entries--

            status (str)
                The status of the endpoint ('OK' or 'ERROR').

            log_levels (map of str to str)
                A map of each log level to its respective value
        """
        self.logger(ranks, log_levels, options)
    # end set_server_logger_level



    def set_client_logger_level( self, log_level ):
        """Set the log level for the client GPUdb class.

        Parameters:
            log_level (int, long, or str)
                A valid log level for the logging module
        """
        try:
            self.log.setLevel( log_level )
        except (ValueError, TypeError, Exception) as ex:
            ex_str = GPUdbException.stringify_exception( ex )
            raise GPUdbException("Invalid log level: '{}'".format( ex_str ))
    # end set_client_logger_level



    # Helper function to emulate old /add (single object insert) capability
    def insert_object(self, set_id, object_data, params=None):
        if (params):
            return self.insert_records(set_id, [object_data], None, params)
        else:
            return self.insert_records(set_id, [object_data], None, {"return_record_ids":"true"})



    def insert_records_from_json(self, json_records, table_name, json_options = None, create_table_options = None, options = None ):
        """Method to insert a single JSON record or an array of JSON records passed in as a string.

        Parameters:
            json_records (str) : Either a single JSON record or an array of JSON records (as string). Mandatory.
            table_name (str) : The name of the table to insert into.
            json_options (dict) : Only valid option is *validate* which could be True or False
            create_table_options (dict) : Same options as the *create_table_options* in :meth:`GPUdb.insert_records_from_payload` endpoint
            options (dict) : Same options as *options* in :meth:`GPUdb.insert_records_from_payload` endpoint

        Example
        ::

            response = gpudb.insert_records_from_json(records, "test_insert_records_json", json_options={'validate': True}, create_table_options={'truncate_table': 'true'})
            response_object = json.loads(response)
            print(response_object['data']['count_inserted'])

        .. seealso:: :meth:`GPUdb.insert_records_from_payload`

        """

        if json_records is None or type(json_records) != str:
            raise GPUdbException("'json_records' must be a parameter of type 'str' and is mandatory")

        if len(json_records) == 0:
            raise GPUdbException("'json_records' must be a valid json and cannot be empty")

        if table_name is None or type(table_name) != str or len(table_name) == 0:
            raise GPUdbException("'table_name' must be a valid non-empty string")

        if json_options and 'validate' in json_options and json_options['validate']:
            if not GPUdb.valid_json( json_records):
                raise GPUdbException("'json_records' passed in is not a valid JSON")

        if create_table_options is None :
            create_table_options = {}

        if options is None or not options:
            options = {'table_name': table_name}

        # overwrite the value
        options['table_name'] = table_name

        combined_options = options if create_table_options is None or not create_table_options else GPUdb.merge_dicts( options, create_table_options )

        query_string = urlencode(combined_options)
        final_endpoint = "/insert/records/json?{}".format(query_string)

        return self.__submit_request_json( final_endpoint, json_records )


    def get_records_json(self, table_name, column_names = None, offset = 0, limit = -9999, expression = None, orderby_columns = None, having_clause = None):
        """ This method is used to retrieve records from a Kinetica table in the form of
        a JSON array (stringified). The only mandatory parameter is the 'tableName'.
        The rest are all optional with suitable defaults wherever applicable.

        Parameters:
            table_name (str): Name of the table
            column_names (list): the columns names to retrieve
            offset (int): the offset to start from - default 0
            limit (int): the maximum number of records - default GPUdb.END_OF_SET
            expression (str): the filter expression
            orderby_columns (list): the list of columns to order by
            having_clause (str): the having clause

        Returns:
            The response string (JSON)

        Raises:
            GPUdbException: On detecting invalid parameters or some other internal errors

        Example
        ::

            resp = gpudb.get_records_json("table_name")
            json_object = json.loads(resp)
            print(json_object["data"]["records"])

        """

        if table_name is None or type(table_name) != str or len(table_name) == 0:
            raise GPUdbException("'table_name' must be a valid non-empty string")

        if column_names is not None and type(column_names) != list:
            raise GPUdbException("'column_names' must be of type 'list'")

        if orderby_columns is not None and type(orderby_columns) != list:
            raise GPUdbException("'orderby_columns' must be of type 'list'")

        if offset is not None and type(offset) != int:
            raise GPUdbException("'offset' must be of type 'int'")

        if limit is not None and type(limit) != int:
            raise GPUdbException("'limit' must be of type 'int'")

        if expression is not None and type(expression) != str:
            raise GPUdbException("'expression' must be of type 'str'")

        if having_clause is not None and type(having_clause) != str:
            raise GPUdbException("'having_clause' must be of type 'str'")

        get_records_json_options = {'table_name': table_name}

        if column_names is not None and len(column_names) != 0:
            get_records_json_options['column_names'] = ','.join(column_names)

        offset = 0 if (offset is None or offset < 0) else offset
        limit = GPUdb.END_OF_SET if (limit is None or limit < 0) else limit

        get_records_json_options['offset'] = offset
        get_records_json_options['limit'] = limit

        if expression is not None and expression != "":
            get_records_json_options['expression'] = expression

        if orderby_columns is not None and len(orderby_columns) != 0:
            get_records_json_options['order_by'] = ','.join(orderby_columns)

        if having_clause is not None and having_clause != "":
            get_records_json_options['having'] = having_clause

        query_string = urlencode(get_records_json_options)
        final_endpoint = "/get/records/json?{}".format(query_string)

        return self.__submit_request_json_without_body( final_endpoint )


    # Helper for dynamic schema responses
    def parse_dynamic_response(self, retobj, do_print=False, convert_nulls = True):

        if (retobj['status_info']['status'] == 'ERROR'):
            return retobj

        my_schema = schema.parse(retobj['response_schema_str'])

        fields = eval(retobj['response_schema_str'])['fields']

        nullable = [type(x['type']['items']) != str for x in fields]

        if len(retobj['binary_encoded_response']) > 0:

            data = retobj['binary_encoded_response']

            # Use the python avro package to decode the data
            decoded = _Util.decode_binary_data( my_schema, data )

            # Translate the column names
            column_lookup = decoded['column_headers']

            translated = collections.OrderedDict()
            for i,(n,column_name) in enumerate(zip(nullable,column_lookup)):

                if (n and convert_nulls): # nullable - replace None with '<NULL>'
                    col = [x if x is not None else '<NULL>' for x in decoded['column_%d'%(i+1)]]
                else:
                    col = decoded['column_%d'%(i+1)]
                # end if

                translated[column_name] = col
            # end loop

            # # TODO: For 7.0, use the following block of code instead of
            # #       the above block (which will now go inside the if block.
            # if "record_type" not in retobj:
            #     # Use the python avro package to decode the data
            #     decoded = _Util.decode_binary_data( my_schema, data )

            #     # Translate the column names
            #     column_lookup = decoded['column_headers']

            #     translated = collections.OrderedDict()
            #     for i,(n,column_name) in enumerate(zip(nullable,column_lookup)):

            #         if (n and convert_nulls): # nullable - replace None with '<NULL>'
            #             col = [x if x is not None else '<NULL>' for x in decoded['column_%d'%(i+1)]]
            #         else:
            #             col = decoded['column_%d'%(i+1)]
            #         # end if

            #         translated[column_name] = col
            #     # end loop

            # else: # use the c-extension for avro decoding
            #     record_type = retobj["record_type"]
            #     if not isinstance( record_type, RecordType ):
            #         raise GPUdbException( "'record_type' must be a RecordType object; given {}"
            #                               "".format( str(type( record_type )) ) )
            #     records = record_type.decode_dynamic_records( data )

            #     # For 6.2, return column-major data
            #     # TODO: For 7.0, just return records, maybe
            #     translated = GPUdbRecord.transpose_data_to_col_major( records )
            # # end if

            retobj['response'] = translated
        else: # JSON encoding
            retobj['response'] = collections.OrderedDict()

            #note running eval here returns a standard (unordered) dict
            #d_resp = eval(retobj['json_encoded_response'])
            d_resp = json.loads(retobj['json_encoded_response'])

            column_lookup = d_resp['column_headers']

            for i,(n,column_name) in enumerate(zip(nullable,column_lookup)):
                column_index_name = 'column_%d'%(i+1)

                #double/float conversion here
                #get the datatype of the underlying data
                data_type = my_schema.fields_dict[column_index_name].type.items.type

                if (data_type == 'double' or data_type == 'float'):
                    retobj['response'][column_name] = [float(x) for x in d_resp[column_index_name]]

                else:
                    retobj['response'][column_name] = d_resp[column_index_name]

                if (n and convert_nulls): # nullable
                    retobj['response'][column_name] = [x if x is not None else '<NULL>' for x in retobj['response'][column_name]]


        if (do_print):
            print(tabulate(retobj['response'],headers='keys',tablefmt='psql'))

        return AttrDict( retobj )
    # end parse_dynamic_response



    def wms( self, wms_params, url = None ):
        """Submits a WMS call to the server.

        Parameters:
            wms_params (str)
                A string containing the WMS endpoint parameters, not containing
                the '/wms' endpoint itself.

            url (str or GPUdb.URL)
                An optional URL to which we submit the /wms endpoint.  If None
                given, use the current URL for this :class:`GPUdb` object.

        Returns:
            A dict with the following entries--

            data
                The /wms content.
            status_info (dict)
                A dict containing more information regarding the request.  Keys:

                * **status**
                * **message**
                * **response_time**
        """
        # Validate the input arguments
        if not url:
            url = self.get_url( stringified = False )
        elif isinstance( url, (basestring, unicode) ):
            try:
                url = GPUdb.URL( url )
            except Exception as ex:
                ex_str = GPUdbException.stringify_exception( ex )
                raise GPUdbException( "Error parsing given URL '{}': {}"
                                      "".format( url, ex_str) )
        elif not isinstance( url, GPUdb.URL ):
            msg = ("Argument 'url' must be a GPUdb.URL object, a string, or None;"
                   " given '{}'".format( str(type(url)) ) )
            self.__log_debug( msg )
            raise GPUdbException( msg )
        # end if

        if not wms_params:
            msg = ("Argument 'wms_params' must be a string; "
                   "given '{}'".format( str(wms_params) ) )
            self.__log_debug( msg )
            raise GPUdbException( msg )
        # end if

        # Make sure that it starts with ?
        if not wms_params.startswith( "?" ):
            wms_params = "?" + wms_params
        # end if

        http_conn = self.__initialize_http_connection( url, self.timeout )

        # WMS is a get, unlike all endpoints which are post
        headers = {
            C._HEADER_ACCEPT: C._REQUEST_ENCODING_JSON
        }
        wms_path = "{url_path}/wms{params}".format( url_path = url.path,
                                                    params   = wms_params )

        # Start shaping up the response
        result = {}
        status_info = {}
        status_info['message'] = ''

        # Actually submit the /wms request
        try:
            # Send the get request
            http_conn.request( C._REQUEST_GET, wms_path, "", headers )
            # Process the response
            raw_response = http_conn.getresponse()
            # Save the response
            result["data"] = raw_response.read()
            # Save ancillary information
            status_info["status"] = "OK"
            status_info["response_time"] = raw_response.getheader( "x-request-time-secs" )
        except Exception as ex:
            # Save the error status and message
            status_info["status"] = "ERROR"
            status_info["message"] = GPUdbException.stringify_exception( ex )
            status_info["response_time"] = raw_response.getheader( "x-request-time-secs" )
        # end try

        result[ "status_info" ] = status_info

        return AttrDict( result )
    # end wms( url )



    def ping( self, url ):
        """Pings the given URL and returns the response.  If no response,
        returns an empty string.

        Parameters:
            url (GPUdb.URL)
                The URL which we are supposed to ping.

        Returns:
            The ping response, or an empty string if it fails.
        """
        # Validate the input arguments
        if isinstance( url, (basestring, unicode) ):
            try:
                url = GPUdb.URL( url )
            except Exception as ex:
                ex_str = GPUdbException.stringify_exception( ex )
                raise GPUdbException( "Error parsing given URL '{}': {}"
                                      "".format( url, ex_str ) )
        elif not isinstance( url, GPUdb.URL ):
            msg = ("Argument 'url' must be a GPUdb.URL object or a string; "
                   "given '{}'".format( str(type(url)) ) )
            self.__log_debug( msg )
            raise GPUdbException( msg )
        # end if

        try:
            http_conn = self.__initialize_http_connection( url, self.__server_connection_timeout )

            # Ping is a get, unlike all endpoints which are post
            headers = {
                C._HEADER_ACCEPT: C._REQUEST_ENCODING_JSON
            }
            http_conn.request( C._REQUEST_GET, url.path, "", headers )

            # Get the ping response
            response = http_conn.getresponse()
            raw_data = response.read()

            # Decode the response, possibly bytes, into string
            if isinstance( raw_data, (basestring, unicode) ):
                # Got a string, no need to decoded
                return raw_data
            elif isinstance( raw_data, bytes ):
                return raw_data.decode("utf-8")
            else:
                raise GPUdbException( "Unhandled response {} with type {}"
                                      "".format( raw_data,
                                                 str(type(raw_data)) ) )
            # end if
        except Exception as ex:
            ex_str = GPUdbException.stringify_exception( ex )
            self.__log_debug( "Got error while pinging: {}".format( ex_str ) )
            return ""
       # end try
    # end ping( url )


    @deprecated
    def is_kinetica_running( self, url ):
        """Verifies that GPUdb is running at the given URL (does not do any HA
        failover).

        Parameters:
            url (GPUdb.URL)
                The URL which we are supposed to ping.

        Returns:
            True if Kinetica is running at that URL, False otherwise.
        """
        ping_response = self.ping( url )
        self.__log_debug( "HTTP server @ {} responded with '{}'"
                          "".format( str(url), ping_response ) )
        if ( ping_response == C._KINETICA_IS_RUNNING ):
            # Kinetica IS running!
            return True
        # end if

        # Did not get the expected response
        return False
    # end is_kinetica_running


    def get_server_debug_information( self, url ):
        """Gets the database debug information from the given URL and returns
        the response.

        Parameters:
            url (GPUdb.URL)
                The URL which we are supposed to get information from.

        Returns:
            The debug response.
        """
        # Validate the input arguments
        if url is None:
            url = self.get_url( stringified = False )
        elif isinstance( url, (basestring, unicode) ):
            try:
                url = GPUdb.URL( url )
            except Exception as ex:
                ex_str = GPUdbException.stringify_exception( ex )
                raise GPUdbException( "Error parsing given URL '{}': {}"
                                      "".format( url, ex_str) )
        elif not isinstance( url, GPUdb.URL ):
            msg = ("Argument 'url' must be a GPUdb.URL object, a string, "
                   "or None; given '{}'".format( str(type(url)) ) )
            self.__log_debug( msg )
            raise GPUdbException( msg )
        # end if

        debug_timeout = 1 # 1 second
        http_conn = self.__initialize_http_connection( url, debug_timeout )

        # Debug is a get, unlike all endpoints which are post
        headers = {
            C._HEADER_ACCEPT: C._REQUEST_ENCODING_JSON
        }
        debug_endpoint = "{}/debug".format( url.path )
        http_conn.request( C._REQUEST_GET, debug_endpoint, "", headers )

        response = http_conn.getresponse()
        return response.read()
    # end get_server_debug_information


    def to_df(self,
              sql: str,
              sql_params: list = [],
              batch_size: int = 5000,
              sql_opts: dict = {},
              show_progress: bool = False):
        """Runs the given query and converts the result to a Pandas Data Frame.

        Parameters:
            sql (str)
                The SQL query to run

            sql_params (list)
                The SQL parameters that will be substituted for tokens (e.g. $1 $2)

            batch_size (int)
                The number of records to retrieve at a time from the database

            sql_opts (dict)
                The options for SQL execution, matching the options passed to
                :meth:`GPUdb.execute_sql`. Defaults to None.

            show_progress (bool)
                Whether to display progress on the console or not. Defaults to False.

        Raises:
            GPUdbException: 

        Returns:
            pd.DataFrame: A Pandas Data Frame containing the result set of the SQL query or None if
                there are no results
        """
        from . import gpudb_dataframe

        return gpudb_dataframe.DataFrameUtils.sql_to_df(self, sql, sql_params, batch_size, sql_opts, show_progress)
    # end to_df


    def query(self, sql, batch_size = 5000, sql_params = [], sql_opts = {}):
        """Execute a SQL query and return a GPUdbSqlIterator

        Parameters:
            sql (str)
                The SQL query to run

            batch_size(int)
                The number of records to retrieve at a time from the database

            sql_params(list of native types)
                The SQL parameters that will be substituted for tokens (e.g. $1 $2)

            sql_opts(dict)
                The options for SQL execution, matching the options passed to
                :meth:`GPUdb.execute_sql`. Defaults to None.

        Returns: 
            An instance of GPUdbSqlIterator.
        """
        from . import gpudb_sql_iterator

        sql_iterator = gpudb_sql_iterator.GPUdbSqlIterator(db=self, 
                sql=sql, 
                batch_size=batch_size, 
                sql_params=sql_params,
                sql_opts=sql_opts)
        
        return sql_iterator
    # end query


    def query_one(self, sql, sql_params = [], sql_opts = {}):
        """Execute a SQL query that returns only one row.

        Parameters:
            sql (str)
                The SQL query to run

            sql_params(list of native types)
                The SQL parameters that will be substituted for tokens (e.g. $1 $2)

            sql_opts(dict)
                The options for SQL execution, matching the options passed to
                :meth:`GPUdb.execute_sql`. Defaults to None.

        Returns: 
            The returned row or None.
        """
        from . import gpudb_sql_iterator

        with gpudb_sql_iterator.GPUdbSqlIterator(db=self, 
                sql=sql, 
                sql_params=sql_params,
                batch_size=2,
                sql_opts=sql_opts) as sql_iterator:
            
            if(sql_iterator.total_count == 0):
                return None
            elif(sql_iterator.total_count > 1):
                raise GPUdbException("More than one result was returned")

            row = sql_iterator.__next__()
            return row
    # end query_one


    def execute(self, sql, sql_params = [], sql_opts = {}):
        """Execute a SQL query and return the row count.

        Parameters:
            sql (str)
                The SQL to execute

            sql_params(list of native types)
                The SQL parameters that will be substituted for tokens (e.g. $1 $2)

            sql_opts(dict)
                The options for SQL execution, matching the options passed to
                :meth:`GPUdb.execute_sql`. Defaults to None.

        Returns:
            Number of records affected
        """

        GPUdb._set_sql_params(sql_opts, sql_params)
        response = self.execute_sql(statement=sql, options=sql_opts)
        GPUdb._check_error(response)
        count_affected = response['count_affected']
        return count_affected
    # end execute


    @classmethod
    def _set_sql_params(cls, sql_opts: dict, sql_params: list) -> None:
        """Convert SQL parameters to JSON and set as an option for execute_sql_and_decode()

        Parameters:
            sql_opts (dict)
                The parameter list that will be appended to.

            sql_params (list of native types)
                The SQL parameters that will be substituted for tokens (e.g. $1 $2)
        """
        if (len(sql_params) == 0):
            return
        
        for idx, item in enumerate(sql_params):
            if (isinstance(item, list)):
                # assume that list type is vector
                sql_params[idx] = str(item)

        sql_opts['query_parameters'] = json.dumps(sql_params)


    @staticmethod
    def get_connection(
            enable_ssl_cert_verification = False, 
            enable_auto_discovery = False,
            enable_failover = False,
            logging_level = 'INFO') -> "GPUdb":
        """ Get a connection to Kinetica getting connection and authentication
        information from environment variables.

        This method is useful particularly for Jupyter notebooks, which won't
        need authentication credentials embedded within them.  This, in turn,
        helps to prevent commit of credentials to the notebook version control.
        In addition, some features including auto-discovery and SSL certificate
        verification are disabled by default to simplify connections for simple
        use cases.

        The following environment variables are required:
        - `KINETICA_URL`: the url of the Kinetica server
        - `KINETICA_USER`: the username to connect with
        - `KINETICA_PASSWD`: the password to connect with

        Parameters:
            enable_ssl_cert_verification (bool):
                Enable SSL certificate verification.
    
            enable_auto_discovery (bool):
                Enable auto-discovery of the initial cluster nodes, as well as
                any attached failover clusters.  This allows for both multi-head
                ingestion & key lookup, as well as cluster failover.
    
            enable_failover (bool):
                Enable failover to another cluster.
    
            logging_level (str):
                Logging level for the connection. (INFO by default)
    
        Returns (GPUdb):
            An active connection to Kinetica.
        """

        ENV_URL = 'KINETICA_URL'
        ENV_USER = 'KINETICA_USER'
        ENV_PASS = 'KINETICA_PASSWD'
        ENV_NOT_FOUND_ERROR = 'Environment variable <{}> needs to be set when connecting with get_connection()'

        if ENV_URL in os.environ:
            url = os.environ[ENV_URL]
        else:
            raise GPUdbException(ENV_NOT_FOUND_ERROR.format( ENV_URL ))

        if ENV_USER in os.environ:
            user = os.environ[ENV_USER]
        else:
            raise GPUdbException(ENV_NOT_FOUND_ERROR.format( ENV_USER ))

        if ENV_PASS in os.environ:
            passwd = os.environ[ENV_PASS]
        else:
            raise GPUdbException(ENV_NOT_FOUND_ERROR.format( ENV_PASS ))

        options = GPUdb.Options()
        options.username = user
        options.password = passwd
        options.skip_ssl_cert_verification = not enable_ssl_cert_verification
        options.disable_auto_discovery = not enable_auto_discovery
        options.disable_failover = not enable_failover
        options.logging_level = logging_level
        kdbc = GPUdb(host = url, options = options)

        kdbc.__log_info("Connected to Kinetica! (host={} api={} server={})".format(kdbc.get_url(), kdbc.api_version, str(kdbc.server_version)))

        return kdbc
    # end get_connection
