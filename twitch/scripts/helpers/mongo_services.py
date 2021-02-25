import logging
from logging.handlers import TimedRotatingFileHandler

from pymongo import MongoClient
from pymongo import errors
from warnings import warn

HOST = 'MONGODB_HOST'
DEFAULT_HOST = 'localhost'
PORT = 'MONGODB_PORT'
DEFAULT_PORT = '27017'
TIMEOUT = 'MONGODB_CONN_TIMEOUT'
DATABASE = 'MONGODB_DB'
COLLECTION = 'MONGODB_DB_CLIENT'
USERNAME = 'MONGODB_USERNAME'
PASSWORD = 'MONGODB_PASSWORD'
LOG_FILE = 'LOG_FILE'


class MongoDB:
    """
    A class used to manage connections to MongoDB
    ...

    Attributes
    ----------
    conf : dict
        a dictionary that has the configuration for class instance.
    client : MongoClient
        the MongoClient from pymongo
    database : pymongo.database.Database
        the database instance from MongoClient
    collection : pymongo.collection.Collection
        the collection instance from database
    verbose: int
        the verbosity of the class (default 5)
    logger: logging.logger
        an logger to log all database operations

    Methods
    -------
    __init_logger__()
        Initializes the logger.
    connect_to_client( host=None, port=None, timeout=3000, username=None, password=None)
        Connects to the Mongodb instance using MongoClient from PyMongo.
    connect_to_database(database_name=None)
        Connects to the collection in the mongodb instance.
    connect_to_collection(collection_name=None)
        Connects to the collection in the mongodb database.
    perform_bulk_operations(list_operations_to_perform_in_bulk=None)
        Executes the operations against MongoDB in bulk.
    """

    def __init__(self, conf, verbose=5):
        self.conf = conf

        self.client = None
        self.database = None
        self.collection = None

        self.verbose = verbose

        self.logger = self.__init_logger__()

    def __init_logger__(self) -> logging.getLogger():
        """This function initializes the logger."""
        logging_file = self.conf.get(LOG_FILE, 'db.log')
        logger = logging.getLogger(__name__)
        log_formatter = logging.Formatter('%(asctime)s|%(name)-12s|%(levelname)-8s|%(message)s')

        log_handler = TimedRotatingFileHandler(filename=f'{logging_file}', when='s', interval=10)
        log_handler.setFormatter(log_formatter)

        logger.addHandler(log_handler)
        logger.setLevel(logging.DEBUG)

        return logger

    def connect_to_client(self, host=None, port=None, timeout=3000, username=None, password=None):
        """This function connects to the Mongodb instance using MongoClient from PyMongo.

        1. If the parameters are not passed they are set to their default value.
        2. Connects to the database.
            Raises OperationFailure if authentication fails.
        3. Testing connection.
            Raises ServerSelectionTimeoutError, if cannot connect to the database in a timely manner.

        Parameters
        ----------
        host: str, optional
            The ip address of the mongodb address. The default 'localhost'.
        port: str, optional
            The port of the mongodb instance. Default is '27017'.
        timeout: int, optional
            The number of seconds to try connecting to the MongoDB instance before timing out. Default is 3 seconds.
        username: str, optional
            The username for authentication. Default is None.
        password: str, optional
            The password for authentication. Default is None.

        Raises
        ------
        errors.ServerSelectionTimeoutError
            If attempt to connect to the server times out.
        errors.OperationFailure
            If authentication with the server fails.
        """

        # 1. If the parameters are not passed they are set to their default value.
        if host is None:
            host = self.conf.get(HOST, host)
            if host is None:
                self.logger.warning(f"No \'{host}\' defined in configuration. Connecting to {DEFAULT_HOST}.")
                host = DEFAULT_HOST

        if port is None:
            port = self.conf.get(PORT, port)
            if port is None:
                self.logger.warning(f"No \'{port}\' defined in configuration. Connecting to {DEFAULT_PORT}.")
                port = DEFAULT_PORT

        connection_host_and_port = f'{host}:{port}'

        if username is None:
            username = self.conf.get(USERNAME, username)

        if password is None:
            password = self.conf.get(PASSWORD, password)

        kwargs = {}
        if username is not None and password is not None:
            kwargs = {
                'username': f"{username}",
                'password': f"{password}",
                'authSource': 'admin',
            }
            msg = f"Username and password are defined in configuration. Connecting with login credentials."
        else:
            msg = f"Username and password are not defined in configuration. Connecting with default credentials."

        if self.verbose >= 1:
            print(msg)
        self.logger.info(msg)

        timeout = self.conf.get(TIMEOUT, timeout)

        try:
            # 2. Connects to the database.
            #   Raises OperationFailure if authentication fails.
            self.client = MongoClient(
                host=connection_host_and_port,  # <-- IP and port go here
                serverSelectionTimeoutMS=timeout,  # 3 se+cond timeout
                **kwargs
            )

            # 3. Testing connection.
            #   Raises ServerSelectionTimeoutError, if cannot connect to the database in a timely manner.
            self.client.server_info()

        except errors.ServerSelectionTimeoutError as err:
            self.logger.error(f'Connection to \'{connection_host_and_port}\' timed out.')
            raise err
        except errors.OperationFailure as err:
            self.logger.error(f'Authentication to \'{connection_host_and_port}\' failed.')
            print(err)
            raise err
        else:
            self.logger.debug(f'Created connection to {connection_host_and_port}')

    def connect_to_database(self, database_name=None):
        """This function connects to the database in the mongodb instance.

        Parameters
        ----------
        database_name: str, optional
            The name of the database. The default 'None'.

        Raises
        ------
        ValueError
            If database name is None.
        """
        if database_name is None:  # Check if database_name is provided.
            database_name = self.conf.get(DATABASE, database_name)  # If it is not provided, get from conf.
            if database_name is None:  # If still none, raise ValueError.
                msg = 'No Database specified.'
                self.logger.error(msg)
                if self.verbose >= 1:
                    warn(msg)
                raise ValueError(msg)

        if database_name not in self.client.database_names():
            msg = f'Database \'{database_name}\' does not exist. Creating database.'
            self.logger.warning(msg)
            if self.verbose >= 1:
                warn(msg)

        self.database = self.client[database_name]
        self.logger.debug(f'Connected to database: \'{database_name}\'')

    def connect_to_collection(self, collection_name=None):
        """This function connects to the collection in the mongodb database.

        Parameters
        ----------
        collection_name: str, optional
            The name of the collection. The default 'None'.

        Raises
        ------
        ValueError
            If collection name is None.
        """
        if collection_name is None:  # Check if collection_name is provided.
            collection_name = self.conf.get(COLLECTION, collection_name)  # If it is not provided, get from conf.
            if collection_name is None:  # If still none, raise ValueError.
                msg = 'No Collection specified.'
                self.logger.error(msg)
                if self.verbose >= 1:
                    warn(msg)
                raise ValueError(msg)

        if collection_name not in self.database.collection_names():
            msg = f'Collection \'{collection_name}\' does not exist. Creating collection.'
            self.logger.warning(msg)
            if self.verbose >= 1:
                warn(msg)

        self.collection = self.database[collection_name]
        self.logger.debug(f'Connected to Collection: \'{collection_name}\'')

    def perform_bulk_operations(self, list_operations_to_perform_in_bulk):
        """This function executes the operations against MongoDB in bulk.

        Parameters
        ----------
        list_operations_to_perform_in_bulk: list, optional 
            The list of operations to perform.

        Raises
        ------
        ValueError
            If requests is empty.
        """
        if not list_operations_to_perform_in_bulk:
            msg = 'No operations to perform.'
            self.logger.error(msg)
            raise ValueError(msg)

        try:
            res = self.collection.bulk_write(list_operations_to_perform_in_bulk, ordered=False)
        except errors.BulkWriteError as bwe:
            self.logger.error(bwe.details['writeErrors'])
            if self.verbose >= 1:
                pass
#                 warn(bwe.details['writeErrors'])
            raise bwe
        else:
            self.logger.info(res.bulk_api_result)
            return res

    def perform_single_fetch(self, fetch_query):
        """This function executes the operations against MongoDB in bulk.

        Parameters
        ----------
        fetch_query: dict, optional
            The dictionary representation of the query to be performed.

        Raises
        ------
        ValueError
            If query is empty.
        """
        if not fetch_query:
            msg = 'No read query to execute.'
            self.logger.error(msg)
            raise ValueError(msg)

        res = self.collection.find(fetch_query)
        return res

    def perform_single_delete(self, delete_query):
        """This function executes the operations against MongoDB in bulk.

        Parameters
        ----------
        delete_query: dict, optional
            The dictionary representation of the query to be performed.

        Raises
        ------
        ValueError
            If query is empty.
        """
        res = self.collection.remove(delete_query)
        return res

    
def get_MongoDB_driver(conf, db, service, verbose=5):
    """


    Parameter
    ---------
    conf: dict,
        Dictionary of configuration parameters for mongo connection.
    db: str,
        The name of the database to use/create.
    service: str,
        The name of the collection to use/create.
    verbose:
        The verbosity of the function.
    """
    mongo_db_object=None
    try:
        mongo_db_object = MongoDB(conf, verbose)
        mongo_db_object.connect_to_client()
        mongo_db_object.connect_to_database(db)
        mongo_db_object.connect_to_collection(service)
    except errors.ServerSelectionTimeoutError as err:
        if verbose >= 0:
            print('Encountered ServerSelectionTimeoutError')
            print(err)
            print(f"Connection to mongoDB instance at \'{conf['MONGODB_HOST']}:{conf['MONGODB_PORT']}\' timed out.")
            print('Exiting program.')
        pass
    except errors.OperationFailure as err:
        if verbose >= 0:
            print('Encountered OperationFailure')
            print(err)
            print(f"Authentication to mongoDB instance at \'{conf['MONGODB_HOST']}:{conf['MONGODB_PORT']}\' failed.")
            print('Exiting program.')
        pass
    except KeyError as err:
        if verbose >= 0:
            print('Encountered KeyError')
            print(err)
            print(f'Services for \'{service}\' do not exist.')
            print('Exiting program.')
        pass
    except ValueError as err:
        if verbose >= 0:
            print('Encountered ValueError')
            print(err)
            print('Exiting program.')
        pass
    else:
        return mongo_db_object