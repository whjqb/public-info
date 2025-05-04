from doc_api import DOCAPIClient
from load_to_postgres import PostgresLoader

if __name__ == "__main__":
    doc_api = DOCAPIClient()
    loader = PostgresLoader()
    
    campsites_alerts_dir = doc_api.get_doc_campsites_alerts()
    if campsites_alerts_dir:
        loader.process_directory(campsites_alerts_dir, 'raw.doc_campsites_alerts', truncate=True)

    campsites_detail_dir = doc_api.get_doc_campsite_detail()
    if campsites_detail_dir:    
        loader.process_directory(campsites_detail_dir, 'raw.doc_campsites_detail', truncate=True)

