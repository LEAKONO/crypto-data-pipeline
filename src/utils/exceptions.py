"""
exceptions.py
-------------
Custom exception classes for the crypto_pipeline project.
"""


class CryptoPipelineError(Exception):
    """
    Base exception for all crypto_pipeline errors.

    All our custom exceptions inherit from this.
    This means you can catch ALL pipeline errors with:
        except CryptoPipelineError as e:
            ...
    Or catch specific ones with:
        except IngestionError as e:
            ...
    """
    pass


class ConfigError(CryptoPipelineError):
    """
    Raised when required configuration or environment
    variables are missing or invalid.
    """
    pass


class IngestionError(CryptoPipelineError):

    #Raised when data cannot be fetched from the CoinGecko API.


    pass


class StorageError(CryptoPipelineError):
    
    #Raised when data cannot be loaded into Snowflake.
  
    pass


class TransformError(CryptoPipelineError):
    pass


class ValidationError(CryptoPipelineError):
    #Raised when data does not pass quality checks.
    pass