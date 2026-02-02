"""/src/ingestion/__init__.py"""

# Handle both direct execution and module import
try:
    from src.ingestion.http_client import HTTPClient
    from src.ingestion.ibge_client import IBGEIngestor
    from src.ingestion.transparency_client import TransparencyIngestor
except ModuleNotFoundError:
    from http_client import HTTPClient
    from ibge_client import IBGEIngestor
    from transparency_client import TransparencyIngestor

__all__ = ['HTTPClient', 'IBGEIngestor', 'TransparencyIngestor']